#include "src/mainwindow.h"
#include "src/style.h"
#include "views/dashboard.h"
#include "views/diagnostics.h"
#include "views/historybar.h"
#include "views/nodeview.h"
#include <QApplication>
#include <QElapsedTimer>
#include <QDateTimeEdit>
#include <QDialog>
#include <QTimer>
#include <QLabel>
#include <QPushButton>
#include <QStyleFactory>
#include <QSlider>
#include <QtTest>
#include <cstdlib>
#include <iostream>

template <typename Condition>
void expect(Condition condition, const char *message, int milliseconds=30000)
{
    QElapsedTimer timer;
    timer.start();
    while (!condition() && timer.elapsed() < milliseconds) QTest::qWait(25);
    if (!condition()) { std::cerr << message << std::endl; std::exit(1); }
}

bool caption(QWidget *root, const QString &value)
{
    for (const auto *label : root->findChildren<QLabel *>())
        if (label->isVisible() && label->text().contains(value)) return true;
    return false;
}

QPushButton *openButton(QWidget *root)
{
    for (auto *item : root->findChildren<QPushButton *>())
        if (item->isVisible() && item->text().contains(QStringLiteral("Открыть ›"))) return item;
    return nullptr;
}

int main(int argc, char **argv)
{
    QApplication app(argc, argv);
    app.setStyle(QStyleFactory::create("Fusion"));
    app.setStyleSheet(v2::style());
    const auto args = app.arguments();
    if (args.size() != 7 || !QStringList{"timeline", "history", "lag"}.contains(args.at(3))) {
        std::cerr << "usage: cluster-archive-vnc-probe CATALOG RUN_ID timeline|history|lag PWSH OUTPUT ARCHIVE_URL\n";
        return 2;
    }
    const bool live = args.value(3) != "history";
    auto runs = readManagedRuns(args.at(1), 18086, 350);
    runs.removeIf([&](const auto &run) { return run.connection.execution != args.at(2); });
    expect([&] { return runs.size() == 1; }, "run not found", 0);
    const auto run = runs.first();
    MainWindow window(run.connection, run.speed, runs);
    window.configureArchiveApi(QUrl(args.at(6)));
    window.show();
    expect([&] { return openButton(&window); }, "open button missing", 1000);
    openButton(&window)->click();
    window.configureManagedRuns(args.at(1), args.at(4));
    auto *history = window.findChild<HistoryBar *>();
    auto *slider = history->findChild<QSlider *>("momentSlider");
    auto *node = window.findChild<NodeView *>();
    auto *dash = window.findChild<Dashboard *>();
    auto *group = window.findChild<Diagnostics *>();
    if (live) {
        expect([&] { return caption(history, QStringLiteral("Ожидание полного среза")); },
               "waiting for complete slice not visible", 1200);
        history->selectTime(run.end);
        expect([&] { return !history->hasSelection(); }, "future selected before first tick", 0);
        std::cout << "waiting_for_complete_slice" << std::endl;
    }
    expect([&] { return history->hasSelection(); }, "no confirmed slice");
    if (live) {
        expect([&] { return slider->maximum() > 0; }, "time picker never offered a processed past moment");
    } else {
        const auto finalStep = run.start.secsTo(run.end) / 10 - 1;
        expect([&] { return slider->maximum() == finalStep; }, "historical range did not synchronize");
    }
    emit history->followRequested(false);
    history->selectTime(run.end);
    const auto available = history->at();
    expect([&] { return available < run.end && (!live || available < run.end.addSecs(-10)); },
           "future timeline selection", 0);
    const auto initialMaximum = slider->maximum();
    if (live) {
        auto *selector = history->findChild<QPushButton *>("timeValue");
        expect([&] { return selector && selector->isEnabled(); }, "manual selector unavailable", 0);
        QTimer::singleShot(0, [&] {
            auto *dialog = qobject_cast<QDialog *>(QApplication::activeModalWidget());
            expect([&] { return dialog; }, "UTC picker did not open", 0);
            auto *editor = dialog->findChild<QDateTimeEdit *>();
            expect([&] { return editor && editor->maximumDateTime() < run.end; },
                   "picker offered future time", 0);
            editor->setDateTime(run.start);
            dialog->accept();
        });
        selector->click();
    } else {
        history->selectTime(run.start);
    }
    emit dash->groupSelected("a14");
    emit group->nodeSelected("a14n04");
    if (args.value(3) == "lag") {
        expect([&] { return caption(node, QStringLiteral("Ожидание подтверждения физической телеметрии")); },
               "no archive pending in Qt");
        expect([&] { return window.grab().save(args.at(5) + "/qt-pending.png"); }, "capture failed", 0);
        std::cout << "waiting_for_archive" << std::endl;
    }
    expect([&] { return caption(node, QStringLiteral("подтверждённые отсчёты")) ||
                         caption(node, QStringLiteral("Проверенный архив Summit D2")); },
           "node did not automatically display confirmed data", 45000);
    expect([&] { return history->at() == run.start && node->node() == "a14n04"; },
           "historical context was lost", 0);
    std::cout << "confirmed_physical_data_without_reopen" << std::endl;
    if (live) {
        expect([&] { return slider->maximum() > initialMaximum; }, "confirmation did not advance", 30000);
        expect([&] { return history->at() == run.start; }, "new results moved historical selection", 0);
        emit history->followRequested(true);
        expect([&] { return caption(history, QStringLiteral("Текущее время телеметрии")); },
               "follow did not resume", 1000);
        std::cout << "historic_context_retained_and_follow_restored" << std::endl;
    } else {
        expect([&] { return available == run.end.addSecs(-10); }, "historical end missing", 0);
        std::cout << "history_restored_after_restart" << std::endl;
    }
    return window.grab().save(args.at(5) + (live ? "/qt-ready.png" : "/qt-history.png")) ? 0 : 1;
}
