#include "models/eventtable.h"
#include "models/nodetable.h"
#include "src/mainwindow.h"
#include "views/dashboard.h"
#include "views/diagnostics.h"
#include "views/historybar.h"

#include <QComboBox>
#include <QLabel>
#include <QPushButton>
#include <QStackedWidget>
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
}

class ViewsTest final : public QObject {
    Q_OBJECT

private slots:
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
        QCOMPARE(workspace->count(), 3);
        QCOMPARE(stack->currentIndex(), 0);
        for (auto *button : window.findChildren<QPushButton *>("navButton"))
            if (button->text() == QStringLiteral("Онлайн-мониторинг")) button->click();
        QCOMPARE(stack->currentIndex(), 1);
        QVERIFY(hasLabel(window, QStringLiteral("MAX")));
        for (auto *button : window.findChildren<QPushButton *>()) {
            if (button->text() == QStringLiteral("Журнал аномалий")) {
                button->click();
                break;
            }
        }
        QCOMPARE(workspace->currentIndex(), 2);
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
        for (const auto *button : dashboard.findChildren<QPushButton *>()) cards << button->text();
        QCOMPARE(cards.size(), 4);
        QVERIFY(cards.at(0).contains(QStringLiteral("Аномалия")));
        QVERIFY(cards.at(1).contains(QStringLiteral("Норма")));
        QVERIFY(cards.at(2).contains(QStringLiteral("Неопределённо")));
        QVERIFY(cards.at(3).contains(QStringLiteral("Нет результата")));

        Diagnostics diagnostics;
        diagnostics.refresh(store, at, at, readTime(last));
        QVERIFY(hasLabel(diagnostics, QStringLiteral("R Норма")));
        QVERIFY(hasLabel(diagnostics, QStringLiteral("R Аномалия")));
        QVERIFY(hasLabel(diagnostics, QStringLiteral("δ 30 с")));
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
        QCOMPARE(events.data(events.index(0, 3), Qt::DisplayRole).toString(), QString("R+B"));
        events.replace({{event.id, event}}, "a02", start, end);
        QCOMPARE(events.rowCount(), 0);
        events.replace({{event.id, event}}, {}, end, end.addSecs(10));
        QCOMPARE(events.rowCount(), 0);
    }
};

QTEST_MAIN(ViewsTest)
#include "views_test.moc"
