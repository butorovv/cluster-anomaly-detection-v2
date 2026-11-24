#include "models/eventtable.h"
#include "models/nodetable.h"
#include "src/mainwindow.h"
#include "src/style.h"
#include "views/dashboard.h"
#include "views/diagnostics.h"
#include "views/historybar.h"
#include "widgets/utcrange.h"
#include "widgets/eventhistogram.h"

#include <QComboBox>
#include <QLabel>
#include <QPushButton>
#include <QScrollArea>
#include <QStackedWidget>
#include <QStyleFactory>
#include <QTableView>
#include <QTemporaryDir>
#include <QtTest>

namespace {
const QString execution = "12345678-1234-4234-8234-123456789abc";
const QString first = "2022-01-18T08:00:00Z";
const QString last = "2022-01-18T08:00:10Z";

GroupTick sample(const QString &group, const QString &vote)
{
    auto raw = QJsonObject{
        {"timestamp", first}, {"group", group}, {"complete_hosts", 1}, {"missing_hosts", QJsonArray{"a01n02"}},
        {"r", QJsonObject{{"state","negative"},{"score",0.85},{"threshold",1.1}}},
        {"b", QJsonObject{{"state","positive"},{"score",2.4},{"threshold",1.9}}},
        {"local", QJsonArray{QJsonObject{{"hostname","a01n01"}, {"is_available",true}, {"is_anomaly",false},
                                         {"anomaly_score",0.27}, {"individual_threshold",0.39}}}}};
    auto temporal = QJsonObject{
        {"vote", vote}, {"delta_seconds", 30}, {"r", QJsonObject{{"state","positive"}}},
        {"b", QJsonObject{{"state","positive"}}}, {"i2", QJsonObject{{"state","unknown"}}},
        {"local_i2", QJsonObject{{"state","unknown"},{"available",1},{"anomalous",0}}},
        {"active_votes", QJsonArray{"R","B"}}};
    return GroupTick::parse(QJsonObject{{"raw",raw},{"temporal",temporal}});
}

bool hasLabel(const QWidget &widget, const QString &text)
{
    const auto labels = widget.findChildren<QLabel *>();
    return std::any_of(labels.begin(), labels.end(), [&text](auto *label) { return label->text().contains(text); });
}

QPushButton *findButton(const QWidget &widget, const QString &text, const QString &role = {})
{
    for (auto *button : widget.findChildren<QPushButton *>())
        if (button->text() == text && (role.isEmpty() || button->objectName() == role)) return button;
    return nullptr;
}
}

class ViewsTest final : public QObject {
    Q_OBJECT

private slots:
    void initTestCase()
    {
        QApplication::setStyle(QStyleFactory::create("Fusion"));
        qApp->setStyleSheet(v2::style());
    }

    void viewNavigationAndModeLabel()
    {
        QTemporaryDir profile;
        QSettings::setDefaultFormat(QSettings::IniFormat);
        QSettings::setPath(QSettings::IniFormat, QSettings::UserScope, profile.path());
        QCoreApplication::setOrganizationName("ClusterMonitorTest");
        MainWindow window({QUrl("http://127.0.0.1:1"), execution, 1000}, "0");
        window.show();
        auto *stack = window.findChild<QStackedWidget *>("mainPages");
        auto *workspace = window.findChild<QStackedWidget *>("workspaceTabs");
        QVERIFY(stack && workspace);
        QCOMPARE(stack->count(), 6);
        QCOMPARE(workspace->count(), 4);
        QCOMPARE(stack->currentIndex(), 0);
        for (auto *button : window.findChildren<QPushButton *>("navButton"))
            if (button->text() == QStringLiteral("Онлайн-мониторинг")) button->click();
        QCOMPARE(stack->currentIndex(), 1);
        QVERIFY(hasLabel(window, QStringLiteral("MAX")));
        auto *dashboard = window.findChild<Dashboard *>();
        QVERIFY(dashboard);
        emit dashboard->journalSelected();
        QCOMPARE(workspace->currentIndex(), 2);
        findButton(window, QStringLiteral("←"), "routeBack")->click();
        QCOMPARE(workspace->currentIndex(), 0);
    }

    void fixedWorkspaceAndRunContext()
    {
        ReviewRun archive;
        archive.connection = {QUrl("http://127.0.0.1:1"), execution, 1000};
        archive.name = archive.shortName = "Archive";
        archive.speed = "0";
        archive.start = readTime(first);
        archive.end = archive.start.addSecs(60);
        archive.verified = true;
        ReviewRun online = archive;
        online.connection.execution = "22222222-2222-4222-8222-222222222222";
        online.speed = "1";
        online.onlineSimulation = true;
        MainWindow window(archive.connection, {}, {archive, online});
        window.show();
        auto *pages = window.findChild<QStackedWidget *>("mainPages");
        QVERIFY(pages);
        auto *open = findButton(*pages->currentWidget(), QStringLiteral("Открыть ›"));
        QVERIFY(open);
        open->click();
        auto *time = window.findChild<QWidget *>("workspaceTime");
        auto *tabBar = window.findChild<QWidget *>("workspaceRoute");
        auto *body = window.findChild<QScrollArea *>("workspaceBody");
        auto *timeline = window.findChild<HistoryBar *>();
        auto *diagnostics = window.findChild<Diagnostics *>();
        QVERIFY(time && tabBar && body && timeline && diagnostics);
        QCoreApplication::processEvents();
        const auto frame = [&window, time, tabBar, body] {
            QList<QRect> bounds;
            for (auto *widget : {time, tabBar, static_cast<QWidget *>(body)})
                bounds << QRect(widget->mapTo(&window, QPoint{}), widget->size());
            return bounds;
        };
        const auto before = frame();
        timeline->selectTime(readTime(last));
        diagnostics->selectGroup("a14");
        diagnostics->setOptions(2, 2);
        const auto checkGeometry = [&] {
            QCoreApplication::processEvents();
            QCOMPARE(frame(), before);
            QVERIFY(!timeline->at().isValid());
        };
        emit window.findChild<Dashboard *>()->groupSelected("a14");
        checkGeometry();
        emit diagnostics->nodeSelected("a14n04");
        checkGeometry();
        findButton(window, QStringLiteral("←"), "routeBack")->click();
        emit diagnostics->journalSelected("a14");
        checkGeometry();
        findButton(window, QStringLiteral("←"), "routeBack")->click();
        checkGeometry();
        auto *live = findButton(window, QStringLiteral("Онлайн-мониторинг"), "navButton");
        QVERIFY(live);
        live->click();
        QVERIFY(findButton(window, QStringLiteral("Выбрать время"), "soft")->isVisible());
        QVERIFY(!findButton(window, QStringLiteral("Выбрать время"), "soft")->isEnabled());
        emit timeline->followRequested(false);
        QVERIFY(findButton(window, QStringLiteral("К текущему"), "soft")->isVisible());
        diagnostics->setOptions(2, 0);
        findButton(window, QStringLiteral("К текущему"), "soft")->click();
        QVERIFY(findButton(window, QStringLiteral("Выбрать время"), "soft")->isVisible());
        QCOMPARE(diagnostics->interval(), 1);
        findButton(window, QStringLiteral("Главная"), "navButton")->click();
        open->click();
        QVERIFY(!timeline->at().isValid());
        QCOMPARE(diagnostics->selectedGroup(), QString("a14"));
        QCOMPARE(diagnostics->criterion(), 2);
        QCOMPARE(diagnostics->interval(), 2);
    }

    void separatesCriteriaAndNodeAbsence()
    {
        MonitorStore store;
        store.summary = {{"group_count",4}};
        store.insert(QList<GroupTick>{sample("a01", "positive"), sample("a02", "negative"), sample("a03", "unknown")});
        const auto at = readTime(first);
        Dashboard dashboard;
        dashboard.refresh(store, at);
        QStringList cards;
        for (const auto *button : dashboard.findChildren<QPushButton *>("groupButton")) cards << button->accessibleName();
        QCOMPARE(cards.size(), 4);
        QVERIFY(cards.at(0).contains(QStringLiteral("Аномалия")));
        QVERIFY(cards.at(1).contains(QStringLiteral("Норма")));
        QVERIFY(cards.at(2).contains(QStringLiteral("Неопределённо")));
        QVERIFY(cards.at(3).contains(QStringLiteral("Нет результата")));

        Diagnostics diagnostics;
        diagnostics.refresh(store, at, at, readTime(last));
        QVERIFY(hasLabel(diagnostics, QStringLiteral("Исходное состояние · Норма")));
        QVERIFY(hasLabel(diagnostics, QStringLiteral("После временной обработки · Аномалия")));
        QVERIFY(hasLabel(diagnostics, QStringLiteral("δ 30 с")));
        diagnostics.show();
        auto *table = diagnostics.findChild<QTableView *>("ifTable");
        QVERIFY(table && !table->isVisible());
        diagnostics.setOptions(2, 1);
        diagnostics.refresh(store, at, at, readTime(last));
        QVERIFY(table->isVisible());
        QVERIFY(hasLabel(diagnostics, QStringLiteral("Динамика критерия I2")));
        NodeTable nodes;
        nodes.setTick(store.tickAt("a01", at));
        QCOMPARE(nodes.rowCount(), 2);
        QCOMPARE(nodes.data(nodes.index(0, 1), Qt::DisplayRole).toString(), QStringLiteral("Норма"));
        QCOMPARE(nodes.data(nodes.index(1, 1), Qt::DisplayRole).toString(), QStringLiteral("Нет результата"));
    }

    void eventIntervalsAndHistoryGrid()
    {
        const auto start = readTime(first);
        const auto end = readTime(last);
        HistoryBar bar;
        bar.setBounds(start, end.addSecs(600));
        bar.setRange(start, end, start.addSecs(19));
        QCOMPARE(bar.at(), end);
        bar.selectTime(start.addSecs(9));
        QCOMPARE(bar.at(), start);

        const AnomalyEvent event{"abc", "a01", start, end, QJsonObject{
            {"duration_seconds",10}, {"vote_segments",QJsonArray{QJsonObject{{"votes","R+B"}}}}}};
        EventTable events;
        events.replace({{event.id, event}}, "a01", start, end);
        QCOMPARE(events.rowCount(), 1);
        QCOMPARE(events.data(events.index(0, 4), Qt::DisplayRole).toString(), QStringLiteral("R · B"));
        events.replace({{event.id, event}}, "a01", start, end, "I2");
        QCOMPARE(events.rowCount(), 0);
        events.replace({{event.id, event}}, {}, start.addSecs(-10), start, "R");
        QCOMPARE(events.rowCount(), 0);
        QVERIFY(EventTable::csv({event}).contains("\"abc\",\"a01\""));
        events.replace({{event.id, event}}, "a02", start, end);
        QCOMPARE(events.rowCount(), 0);
        events.replace({{event.id, event}}, {}, end, end.addSecs(10));
        QCOMPARE(events.rowCount(), 0);

        MonitorStore store;
        store.insert(QList<AnomalyEvent>{event});
        Events journal;
        journal.refresh(store, start, end.addSecs(30));
        QSignalSpy signal(&journal, &Events::eventSelected);
        findButton(journal, QStringLiteral("Открыть в диагностике"), "primary")->click();
        QCOMPARE(signal.size(), 1);
        QCOMPARE(signal.first().at(0).toString(), QString("a01"));
        QCOMPARE(signal.first().at(1).toDateTime(), start);
        QCOMPARE(signal.first().at(2).toDateTime(), end);
    }

    void validatesUtcPeriodAndSelectsHistogramBin()
    {
        const auto start = QDateTime::fromString("2022-01-18T23:59:00Z", Qt::ISODate);
        const auto end = start.addSecs(120);
        UtcRange range("Начало", "Конец");
        range.setBounds(start, end);
        range.setPeriod(start, end);
        QCOMPARE(range.from(), start);
        QCOMPARE(range.to(), end);
        QVERIFY(range.validate(10));
        range.setPeriod(start.addSecs(1), end);
        QVERIFY(!range.validate(10));
        range.setPeriod(end, start);
        QVERIFY(!range.validate());
        EventHistogram histogram;
        histogram.resize(800, 131);
        histogram.setData({start.addSecs(1)}, start, end, {}, {});
        QSignalSpy selection(&histogram, &EventHistogram::intervalSelected);
        QTest::mouseClick(&histogram, Qt::LeftButton, {}, QPoint(30, 80));
        QCOMPARE(selection.size(), 1);
        QCOMPARE(selection.first().first().toDateTime(), start);
    }
};

QTEST_MAIN(ViewsTest)
#include "views_test.moc"
