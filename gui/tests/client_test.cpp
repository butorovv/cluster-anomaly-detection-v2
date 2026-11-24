#include "api/poller.h"

#include <QHostAddress>
#include <QTcpServer>
#include <QTcpSocket>
#include <QtTest>

namespace {
const QString execution = "11111111-1111-4111-8111-111111111111";
const QString first = "2022-01-18T08:00:00Z";
const QString middle = "2022-01-18T08:00:10Z";
const QString last = "2022-01-18T08:00:20Z";

QJsonObject tick(const QString &time, const QString &vote = "negative", const QString &group = "a01")
{
    return {{"raw", QJsonObject{{"group", group}, {"timestamp", time}}},
            {"temporal", QJsonObject{{"vote", vote}}}};
}

QJsonObject eventPayload(const QString &id)
{
    return {{"id", id}, {"group", "a01"}, {"detected", first}, {"closed", last}};
}

GroupTick parsedTick(const QString &time, const QString &vote = "negative", const QString &group = "a01")
{
    return GroupTick::parse(tick(time, vote, group));
}

class MasterFixture : public QTcpServer {
public:
    MasterFixture()
    {
        connect(this, &QTcpServer::newConnection, this, [this] {
            while (auto *socket = nextPendingConnection()) {
                connect(socket, &QTcpSocket::disconnected, socket, &QObject::deleteLater);
                connect(socket, &QTcpSocket::readyRead, this, [this, socket] {
                    const auto available = socket->peek(socket->bytesAvailable());
                    if (!available.contains("\r\n\r\n")) return;
                    socket->readAll();
                    const auto url = QUrl::fromEncoded(available.split(' ').value(1));
                    send(socket, url);
                });
            }
        });
    }

    bool failing = false;
    bool late = false;
    bool summaryFailing = false;
    bool newTail = false;

private:
    void send(QTcpSocket *socket, const QUrl &url)
    {
        const auto path = url.path();
        const QUrlQuery query(url);
        const auto selected = query.queryItemValue("execution_id");
        QJsonDocument body;
        if (path == "/v1/summary") {
            body = QJsonDocument(QJsonObject{{"execution_id", selected}, {"group_count", 1}, {"window_start", first},
                {"group_ticks", selected == execution ? (late ? 3 : 2) : 0},
                {"parent_events", selected == execution ? (late ? 2 : 1) : 0}});
        } else if (path == "/v1/results") {
            const auto after = query.queryItemValue("after_tick");
            auto times = late ? QStringList{first, middle, last} : QStringList{first, last};
            if (newTail) times << "2022-01-18T08:00:30Z";
            QJsonArray items;
            QString next = after;
            for (const auto &time : times) {
                if (selected == execution && time > after) {
                    items.append(tick(time, time == middle ? QString("positive") : QString("negative")));
                    next = time;
                    break; // A short page may still have a successor.
                }
            }
            body = QJsonDocument(QJsonObject{{"items", items}, {"next", QJsonObject{
                {"tick", next.isEmpty() ? QString("0001-01-01T00:00:00Z") : next}, {"group_id", next.isEmpty() ? "" : "a01"}}}});
        } else if (path == "/v1/events") {
            const auto after = query.queryItemValue("after");
            QJsonArray rows;
            auto ids = late ? QStringList{"a", "z"} : QStringList{"z"};
            if (newTail) ids << "zz";
            for (const auto &id : ids)
                if (selected == execution && id > after) rows.append(eventPayload(id));
            body = QJsonDocument(rows);
        } else if (path == "/metrics") {
            body = QJsonDocument(QJsonObject{});
        }
        const auto bytes = body.isNull() ? QByteArray{} : body.toJson(QJsonDocument::Compact);
        const bool unavailable = failing || (summaryFailing && path == "/v1/summary");
        socket->write(QByteArray(unavailable ? "HTTP/1.1 503 Unavailable\r\n" : "HTTP/1.1 200 OK\r\n")
                      + "Content-Length: " + QByteArray::number(bytes.size()) + "\r\nConnection: close\r\n\r\n" + bytes);
        socket->disconnectFromHost();
    }
};
}

class ClientTest : public QObject {
    Q_OBJECT
private slots:
    void statesAndConfirmedGrid()
    {
        MonitorStore store;
        store.summary = {{"group_count", 2}, {"window_start", first}};
        const auto t = parsedTick(first, "unknown");
        store.insert(QList<GroupTick>{t, t, parsedTick(last, "positive", "a02")});
        QCOMPARE(store.resultCount(), 2);
        QCOMPARE(t.state(), GroupState::Unknown);
        QCOMPARE(store.tickAt("a02", t.time), nullptr);
        QCOMPARE(store.groups(), QStringList({"a01", "a02"}));
        QVERIFY(store.confirmedCursor().tick.isEmpty());
        store.insert(QList<GroupTick>{parsedTick(first, "negative", "a02"), parsedTick(last)});
        QCOMPARE(store.confirmedCursor().tick, first);
        QCOMPARE(store.confirmedCursor().group, QString("a02"));
        QCOMPARE(store.recentAnomalies(store.latestTime(), 10).size(), 1);
        store.insert(QList<GroupTick>{parsedTick(middle), parsedTick(middle, "negative", "a02")});
        QCOMPARE(store.confirmedCursor().tick, last);
    }

    void rejectsBadCursors()
    {
        const auto payload = QJsonDocument(QJsonObject{{"items", QJsonArray{tick(first)}},
            {"next", QJsonObject{{"tick", first}, {"group_id", "a01"}}}});
        QVERIFY(ResultPage::parse(payload, {}));
        QVERIFY(!ResultPage::parse(payload, {first, "a01"}));
        QVERIFY(!AnomalyEvent::parse(QJsonDocument(QJsonArray{eventPayload("a"), eventPayload("a")}), {}));
    }

    void readsLiveFeedsWhileSummaryIsUnavailable()
    {
        MasterFixture fixture;
        QVERIFY(fixture.listen(QHostAddress::LocalHost));
        Poller poller;
        poller.configure({QUrl(QString("http://127.0.0.1:%1").arg(fixture.serverPort())), execution, 50});
        QTRY_VERIFY_WITH_TIMEOUT(poller.synchronized(), 4000);
        fixture.summaryFailing = true;
        fixture.newTail = true;
        QTRY_VERIFY_WITH_TIMEOUT(poller.errors().contains("/v1/summary"), 4000);
        QTRY_COMPARE_WITH_TIMEOUT(poller.store().resultCount(), 3, 4000);
        QTRY_COMPARE_WITH_TIMEOUT(poller.store().events().size(), 2, 4000);
        QCOMPARE(poller.health(), 1);
    }

    void recoversLatePagesAndIsolatesExecutions()
    {
        MasterFixture fixture;
        QVERIFY2(fixture.listen(QHostAddress::LocalHost), qPrintable(fixture.errorString()));
        Poller poller;
        ConnectionSettings settings{QUrl(QString("http://127.0.0.1:%1").arg(fixture.serverPort())), execution, 50};
        poller.configure(settings);
        QTRY_COMPARE_WITH_TIMEOUT(poller.store().resultCount(), 2, 4000);
        QTRY_COMPARE_WITH_TIMEOUT(poller.store().events().size(), 1, 4000);
        fixture.failing = true;
        QTRY_VERIFY_WITH_TIMEOUT(poller.stale(), 4000);
        QCOMPARE(poller.store().resultCount(), 2);
        fixture.late = true;
        fixture.failing = false;
        QTRY_COMPARE_WITH_TIMEOUT(poller.store().resultCount(), 3, 4000);
        QTRY_COMPARE_WITH_TIMEOUT(poller.store().events().size(), 2, 4000);
        QTRY_VERIFY_WITH_TIMEOUT(poller.synchronized(), 4000);
        QCOMPARE(poller.store().tickAt("a01", readTime(middle))->state(), GroupState::Positive);
        settings.execution = "22222222-2222-4222-8222-222222222222";
        poller.configure(settings);
        QTRY_VERIFY_WITH_TIMEOUT(poller.synchronized(), 4000);
        QCOMPARE(poller.store().resultCount(), 0);
        QCOMPARE(poller.store().events().size(), 0);
    }

    void realMaster()
    {
        const auto endpoint = qEnvironmentVariable("CAD_GUI_TEST_MASTER");
        const auto id = qEnvironmentVariable("CAD_GUI_TEST_EXECUTION");
        if (endpoint.isEmpty() || id.isEmpty())
            QSKIP("Set CAD_GUI_TEST_MASTER and CAD_GUI_TEST_EXECUTION for the opt-in REST integration check.");
        Poller poller;
        bool seenAnomalyDuringRun = false;
        connect(&poller, &Poller::updated, this, [&] {
            const auto &store = poller.store();
            if (!store.summary.value("complete").toBool()
                && !store.recentAnomalies(store.latestTime(), 1).isEmpty())
                seenAnomalyDuringRun = true;
        });
        poller.configure({QUrl(endpoint), id, 1000});
        QTRY_COMPARE_WITH_TIMEOUT(poller.readiness(), 1, 15000);
        QTRY_VERIFY_WITH_TIMEOUT(poller.store().summary.value("complete").toBool() && poller.synchronized(), 900000);
        const auto &store = poller.store();
        QCOMPARE(store.resultCount(), store.summary.value("expected_ticks").toInteger());
        QCOMPARE(store.events().size(), store.summary.value("parent_events").toInteger());
        const auto lastTick = readTime(store.summary.value("window_end")).addSecs(-10);
        QCOMPARE(store.latestTime(), lastTick);
        const auto firstTick = readTime(store.summary.value("window_start"));
        qint64 positive = 0;
        qint64 unknown = 0;
        for (const auto &group : store.groups()) {
            auto next = firstTick;
            for (const auto &tick : store.timeline(group)) {
                QCOMPARE(tick.time, next);
                next = next.addSecs(10);
                positive += tick.state() == GroupState::Positive;
                unknown += tick.state() == GroupState::Unknown;
            }
            QCOMPARE(next.addSecs(-10), lastTick);
        }
        QCOMPARE(positive, store.summary.value("positive_ticks").toInteger());
        QCOMPARE(unknown, store.summary.value("unknown_ticks").toInteger());
        QVERIFY(!store.recentAnomalies(store.latestTime(), 1).isEmpty());
        if (qEnvironmentVariableIsSet("CAD_GUI_TEST_REQUIRE_LIVE")) QVERIFY(seenAnomalyDuringRun);
        qInfo() << "Master" << store.resultCount() << "ticks" << store.events().size()
                << "events; anomaly before completion:" << seenAnomalyDuringRun;
    }
};

QTEST_GUILESS_MAIN(ClientTest)
#include "client_test.moc"
