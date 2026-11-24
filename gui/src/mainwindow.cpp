#include "mainwindow.h"
#include "views/dashboard.h"
#include "views/diagnostics.h"
#include "views/events.h"
#include "views/historybar.h"
#include "views/settings.h"
#include "views/system.h"

#include <QButtonGroup>
#include <QComboBox>
#include <QHBoxLayout>
#include <QLabel>
#include <QLineEdit>
#include <QPushButton>
#include <QSignalBlocker>
#include <QStackedWidget>
#include <QVBoxLayout>
#include <array>

MainWindow::MainWindow(const ConnectionSettings &initial, const QString &summitSpeed, QWidget *parent)
    : QMainWindow(parent), summitSpeed_(summitSpeed)
{
    setWindowTitle(QStringLiteral("Диагностика вычислительного кластера"));
    resize(1180, 860);
    auto *workspace = new QWidget;
    workspace->setObjectName("workspace");
    setCentralWidget(workspace);
    auto *layout = new QVBoxLayout(workspace);
    layout->setContentsMargins(22, 16, 22, 16);
    layout->setSpacing(12);
    auto *title = new QLabel(QStringLiteral("Диагностика кластера"));
    title->setObjectName("title");
    layout->addWidget(title);
    auto *header = new QHBoxLayout;
    connection_ = new QLabel;
    execution_ = new QLineEdit;
    execution_->setMaxLength(36);
    execution_->setMinimumWidth(290);
    execution_->setPlaceholderText(tr("Execution ID"));
    execution_->setAccessibleName(tr("Execution ID"));
    auto *apply = new QPushButton(tr("Подключить"));
    header->addWidget(connection_);
    header->addStretch();
    header->addWidget(execution_);
    header->addWidget(apply);
    mode_ = new QComboBox;
    mode_->setAccessibleName(tr("Режим просмотра"));
    mode_->addItems({"LIVE", "FAST REPLAY", "HISTORY"});
    mode_->setCurrentIndex(summitSpeed_.isEmpty() || summitSpeed_ == "1" ? Live : FastReplay);
    mode_->setToolTip(tr("Режим просмотра. Темп Replayer задаётся при его запуске."));
    header->addWidget(mode_);
    layout->addLayout(header);
    source_ = new QLabel;
    source_->setObjectName("muted");
    layout->addWidget(source_);
    const auto applyExecution = [this] {
        auto settings = poller_.settings();
        settings.execution = execution_->text().trimmed().toLower();
        if (MasterClient::validExecution(settings.execution) || settings.execution.isEmpty()) {
            applyConnection(settings);
        } else {
            execution_->setStyleSheet("border-color:#b42838");
            execution_->setToolTip(tr("Укажите Execution ID в формате UUID."));
        }
    };
    connect(apply, &QPushButton::clicked, this, applyExecution);
    connect(execution_, &QLineEdit::returnPressed, this, applyExecution);
    history_ = new HistoryBar;
    history_->hide();
    layout->addWidget(history_);
    auto *navigation = new QHBoxLayout;
    layout->addLayout(navigation);
    navigation_ = new QButtonGroup(this);
    pages_ = new QStackedWidget;
    dashboard_ = new Dashboard;
    diagnostics_ = new Diagnostics;
    events_ = new Events;
    system_ = new SystemView;
    const QStringList names{QStringLiteral("Обзор кластера"), QStringLiteral("Диагностика группы"),
                            QStringLiteral("Журнал аномалий"), QStringLiteral("Состояние системы")};
    const std::array<QWidget *, 4> views{dashboard_, diagnostics_, events_, system_};
    for (size_t i = 0; i < views.size(); ++i) {
        auto *button = new QPushButton(names.at(i));
        button->setCheckable(true);
        navigation_->addButton(button, pages_->addWidget(views[i]));
        navigation->addWidget(button);
    }
    navigation_->button(0)->setChecked(true);
    connect(navigation_, &QButtonGroup::idClicked, this, &MainWindow::showPage);
    connect(dashboard_, &Dashboard::groupSelected, this, &MainWindow::selectGroup);
    connect(dashboard_, &Dashboard::historySelected, this, [this](const QString &group, const QDateTime &at) {
        openHistory(group, at.addSecs(-600), at.addSecs(600), at);
    });
    connect(events_, &Events::eventSelected, this, [this](const QString &group, const QDateTime &from, const QDateTime &to) {
        openHistory(group, from, to, from);
    });
    connect(diagnostics_, &Diagnostics::timeSelected, this, [this](const QDateTime &at) {
        if (mode_->currentIndex() == History) {
            history_->selectTime(at);
        } else {
            const auto latest = poller_.store().latestTime();
            openHistory({}, latest.addSecs(-1800), latest, at);
        }
    });
    connect(diagnostics_, &Diagnostics::selectionChanged, this, &MainWindow::refresh);
    connect(events_, &Events::filterChanged, this, &MainWindow::refresh);
    navigation->addStretch();
    auto *settings = new QPushButton(QStringLiteral("Настройки"));
    navigation->addWidget(settings);
    connect(settings, &QPushButton::clicked, this, [this] {
        SettingsDialog dialog(poller_.settings(), this);
        if (dialog.exec() == QDialog::Accepted) applyConnection(dialog.settings());
    });
    layout->addWidget(pages_, 1);
    connect(history_, &HistoryBar::changed, this, &MainWindow::refresh);
    connect(mode_, &QComboBox::currentIndexChanged, this, &MainWindow::refresh);
    refreshTimer_.setSingleShot(true);
    refreshTimer_.setInterval(200);
    connect(&refreshTimer_, &QTimer::timeout, this, &MainWindow::refresh);
    connect(&poller_, &Poller::updated, this, [this] {
        if (!refreshTimer_.isActive()) refreshTimer_.start();
    });
    connect(&poller_, &Poller::reset, this, [this] {
        history_->setBounds({}, {});
        events_->clear();
        refresh();
    });
    applyConnection(initial);
}

void MainWindow::showPage(int index)
{
    navigation_->button(index)->setChecked(true);
    pages_->setCurrentIndex(index);
    refresh();
}

void MainWindow::refresh()
{
    updateConnection();
    updateHistory();
    const auto &store = poller_.store();
    const bool history = mode_->currentIndex() == History;
    const auto at = history ? history_->at() : store.latestTime();
    if (pages_->currentWidget() == dashboard_)
        dashboard_->refresh(store, at);
    else if (pages_->currentWidget() == diagnostics_)
        diagnostics_->refresh(store, at, history ? history_->from() : qMax(store.earliestTime(), at.addSecs(-1800)),
                              history ? history_->to() : at);
    else if (pages_->currentWidget() == events_)
        events_->refresh(store, history ? history_->from() : QDateTime{}, history ? history_->to() : QDateTime{});
    else if (pages_->currentWidget() == system_)
        system_->refresh(poller_);
}

void MainWindow::updateHistory()
{
    const bool active = mode_->currentIndex() == History;
    history_->setVisible(active);
    if (!active) return;
    const auto &store = poller_.store();
    const auto start = readTime(store.summary.value("window_start"));
    const auto end = readTime(store.summary.value("window_end")).addSecs(-10);
    history_->setBounds(start.isValid() ? start : store.earliestTime(), end.isValid() ? end : store.latestTime());
    if (!history_->hasSelection() && (start.isValid() || store.earliestTime().isValid())) {
        const auto latest = store.latestTime().isValid() ? store.latestTime() : start;
        const QSignalBlocker blocker(history_);
        history_->setRange(start.isValid() ? start : store.earliestTime(), latest, latest);
    }
}

void MainWindow::selectGroup(const QString &group)
{
    showPage(1);
    diagnostics_->selectGroup(group);
    refresh();
}

void MainWindow::openHistory(const QString &group, const QDateTime &from, const QDateTime &to, const QDateTime &at)
{
    const QSignalBlocker blocker(mode_);
    const QSignalBlocker historyBlocker(history_);
    mode_->setCurrentIndex(History);
    updateHistory();
    history_->setRange(from, to, at);
    if (group.isEmpty()) refresh();
    else selectGroup(group);
}

void MainWindow::applyConnection(const ConnectionSettings &settings)
{
    if (!poller_.settings().execution.isEmpty()
        && (settings.execution != poller_.settings().execution || settings.masterUrl != poller_.settings().masterUrl)) {
        summitSpeed_.clear();
        const QSignalBlocker blocker(mode_);
        mode_->setCurrentIndex(Live);
    }
    execution_->setText(settings.execution);
    execution_->setStyleSheet({});
    execution_->setToolTip({});
    preferences_.setValue("masterUrl", settings.masterUrl.toString());
    preferences_.setValue("execution", settings.execution);
    preferences_.setValue("intervalMs", settings.intervalMs);
    poller_.configure(settings);
}

void MainWindow::updateConnection()
{
    const int health = poller_.health();
    const bool stale = poller_.stale();
    connection_->setText(health < 0 ? tr("Проверка Master…")
        : health == 0 ? tr("Связь с Master потеряна")
        : stale ? tr("Master доступен · данные неактуальны") : tr("Master подключён"));
    connection_->setStyleSheet(health == 1 && !stale ? "color:#157149;font-weight:bold"
                                                     : "color:#95501e;font-weight:bold");
    const auto pace = summitSpeed_ == "0" ? QStringLiteral("MAX") : QStringLiteral("×") + summitSpeed_;
    source_->setText(summitSpeed_.isEmpty() ? tr("Источник и темп не указаны")
        : tr("Источник — воспроизведение исторической телеметрии Summit   ·   Темп по конфигурации %1").arg(pace));
}
