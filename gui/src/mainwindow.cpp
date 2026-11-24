#include "mainwindow.h"
#include "style.h"
#include "views/dashboard.h"
#include "views/diagnostics.h"
#include "views/events.h"
#include "views/historybar.h"
#include "views/settings.h"
#include "views/system.h"
#include "views/viewhelpers.h"

#include <QButtonGroup>
#include <QComboBox>
#include <QFrame>
#include <QHBoxLayout>
#include <QLabel>
#include <QLineEdit>
#include <QPushButton>
#include <QScrollArea>
#include <QScrollBar>
#include <QSignalBlocker>
#include <QStackedWidget>
#include <QVBoxLayout>

namespace {
class WorkspaceStack final : public QStackedWidget {
public:
    QSize sizeHint() const override { return currentWidget() ? currentWidget()->sizeHint() : QSize{}; }
    QSize minimumSizeHint() const override { return currentWidget() ? currentWidget()->minimumSizeHint() : QSize{}; }
};

QLabel *label(const QString &text, const char *role = "")
{
    auto *value = new QLabel(text);
    value->setTextFormat(Qt::PlainText);
    value->setObjectName(QString::fromLatin1(role));
    return value;
}

QPushButton *button(const QString &text, const char *role = "")
{
    auto *value = new QPushButton(text);
    value->setObjectName(QString::fromLatin1(role));
    return value;
}

QFrame *panel()
{
    auto *frame = new QFrame;
    frame->setObjectName("panel");
    return frame;
}

QVBoxLayout *column(QWidget *parent, int margin = 0, int spacing = 12)
{
    auto *layout = new QVBoxLayout(parent);
    layout->setContentsMargins(margin, margin, margin, margin);
    layout->setSpacing(spacing);
    return layout;
}

void heading(QVBoxLayout *layout, const QString &title, const QString &subtitle)
{
    auto *header = new QWidget;
    auto *content = column(header, 0, 6);
    content->addWidget(label(title, "title"));
    content->addWidget(label(subtitle, "muted"));
    layout->addWidget(header);
    layout->addSpacing(18);
}

QWidget *scrollPage(QWidget *content)
{
    auto *scroll = new QScrollArea;
    scroll->setFrameShape(QFrame::NoFrame);
    scroll->setWidgetResizable(true);
    scroll->setHorizontalScrollBarPolicy(Qt::ScrollBarAlwaysOff);
    scroll->setWidget(content);
    return scroll;
}

QString pace(const QString &speed)
{
    if (speed.isEmpty()) return {};
    return speed == "0" ? QStringLiteral("MAX") : QStringLiteral("×") + speed;
}

QString runKey(const ReviewRun &run)
{
    return run.connection.masterUrl.toString() + '|' + run.connection.execution;
}
}

MainWindow::MainWindow(const ConnectionSettings &initial, const QString &summitSpeed,
                       QList<ReviewRun> runs, QWidget *parent)
    : QMainWindow(parent), runs_(std::move(runs))
{
    attached_.connection = initial;
    attached_.speed = summitSpeed;
    attached_.name = tr("Подключённый запуск");
    current_ = attached_;
    setWindowTitle(tr("Cluster Monitor · V2"));
    resize(1280, 900);
    setMinimumSize(1040, 690);

    auto *root = new QWidget;
    auto *shell = new QHBoxLayout(root);
    shell->setContentsMargins(0, 0, 0, 0);
    shell->setSpacing(0);
    setCentralWidget(root);
    auto *sidebar = new QWidget;
    sidebar->setObjectName("sidebar");
    sidebar->setFixedWidth(220);
    auto *side = column(sidebar, 13, 3);
    side->setContentsMargins(13, 29, 13, 18);
    auto *brand = new QWidget;
    auto *brandLayout = column(brand, 0, 4);
    brandLayout->setContentsMargins(12, 0, 0, 23);
    brandLayout->addWidget(label("Cluster Monitor", "brand"));
    brandLayout->addWidget(label(tr("Диагностика кластера"), "sideCaption"));
    side->addWidget(brand);
    navigation_ = new QButtonGroup(this);
    const QStringList sections{tr("Главная"), tr("Онлайн-мониторинг"), tr("Анализ архива"),
                               tr("История запусков"), tr("Состояние системы"), tr("Настройки")};
    for (int i = 0; i < sections.size(); ++i) {
        if (i == Online || i == System) {
            auto *caption = label(i == Online ? tr("ДИАГНОСТИКА") : tr("СИСТЕМА"), "navHeading");
            caption->setContentsMargins(12, 21, 0, 5);
            side->addWidget(caption);
        }
        auto *item = button(sections[i], "navButton");
        item->setIcon(v2::icon(i, QColor("#c5d1df")));
        item->setFixedHeight(43);
        item->setCheckable(true);
        navigation_->addButton(item, i);
        side->addWidget(item);
    }
    side->addStretch();
    connection_ = label({});
    connection_->setContentsMargins(12, 0, 8, 0);
    connection_->setWordWrap(true);
    side->addWidget(connection_);
    auto *rest = label(tr("REST API"), "sideCaption");
    rest->setContentsMargins(12, 4, 8, 0);
    side->addWidget(rest);
    shell->addWidget(sidebar);

    auto *surface = new QWidget;
    surface->setObjectName("surface");
    auto *main = column(surface);
    main->setContentsMargins(34, 22, 49, 15);
    pages_ = new QStackedWidget;
    pages_->setObjectName("mainPages");
    pages_->addWidget(scrollPage(buildHome()));
    pages_->addWidget(buildWorkspace());
    pages_->addWidget(scrollPage(buildArchive()));
    pages_->addWidget(scrollPage(buildCatalog()));
    system_ = new SystemView;
    pages_->addWidget(system_);
    pages_->addWidget(scrollPage(buildSettings()));
    main->addWidget(pages_);
    shell->addWidget(surface, 1);

    connect(navigation_, &QButtonGroup::idClicked, this, [this](int index) { navigate(Section(index)); });
    refreshTimer_.setSingleShot(true);
    refreshTimer_.setInterval(200);
    connect(&refreshTimer_, &QTimer::timeout, this, &MainWindow::refresh);
    connect(&poller_, &Poller::updated, this, [this] {
        if (!refreshTimer_.isActive()) refreshTimer_.start();
    });
    connect(&poller_, &Poller::reset, this, &MainWindow::refresh);
    navigate(Home);
    poller_.configure(runs_.isEmpty() ? initial : runs_.first().connection);
}

QWidget *MainWindow::buildHome()
{
    auto *page = new QWidget;
    page->setObjectName("surface");
    auto *layout = column(page);
    layout->addSpacing(7);
    heading(layout, tr("Диагностика кластера"), tr("Выберите сценарий работы"));
    auto *tiles = new QHBoxLayout;
    tiles->setSpacing(16);
    for (const bool online : {true, false}) {
        auto *tile = panel();
        tile->setFixedHeight(223);
        auto *body = column(tile, 20, 10);
        auto *badge = label({});
        badge->setPixmap(v2::icon(online ? 1 : 2, QColor(online ? "#16866b" : "#295ec5"), 23).pixmap(23));
        badge->setAlignment(Qt::AlignCenter);
        badge->setFixedSize(36, 36);
        badge->setStyleSheet(online ? "background:#e4f5ed;border-radius:18px" : "background:#edf3fc;border-radius:18px");
        body->addWidget(badge);
        body->addWidget(label(online ? tr("Онлайн-мониторинг") : tr("Анализ архива"), "tileTitle"));
        auto *description = label(online ? tr("Наблюдение за состоянием групп по мере поступления телеметрии.")
                                         : tr("Обработка сохранённой телеметрии за выбранный период."), "muted");
        description->setWordWrap(true);
        description->setMaximumWidth(310);
        body->addWidget(description);
        body->addStretch();
        auto *action = button(online ? tr("Перейти к мониторингу") : tr("Новый запуск"), online ? "soft" : "primary");
        action->setFixedHeight(40);
        body->addWidget(action);
        connect(action, &QPushButton::clicked, this, [this, online] { navigate(online ? Online : Archive); });
        tiles->addWidget(tile, 1);
    }
    layout->addLayout(tiles);
    layout->addSpacing(12);
    auto *recent = new QHBoxLayout;
    recent->addWidget(sectionLabel(tr("Последние запуски")));
    recent->addStretch();
    auto *all = button(tr("Вся история ›"), "link");
    connect(all, &QPushButton::clicked, this, [this] { navigate(Catalog); });
    recent->addWidget(all);
    layout->addLayout(recent);
    auto *table = panel();
    auto *rows = column(table, 1, 0);
    auto *header = new QFrame;
    header->setObjectName("tableHead");
    auto *columns = new QHBoxLayout(header);
    columns->setContentsMargins(15, 0, 15, 0);
    header->setFixedHeight(42);
    columns->addWidget(label(tr("ЗАПУСК"), "small"), 42);
    columns->addWidget(label(tr("СОБЫТИЯ"), "small"), 18);
    columns->addWidget(label(tr("СОСТОЯНИЕ"), "small"), 26);
    columns->addWidget(label({}), 14);
    rows->addWidget(header);
    if (runs_.isEmpty()) {
        auto *empty = label(tr("Сохранённых запусков пока нет"), "muted");
        empty->setAlignment(Qt::AlignCenter);
        empty->setMinimumHeight(68);
        rows->addWidget(empty);
    }
    for (const auto &run : runs_.mid(0, 2)) {
        auto *row = new QFrame;
        auto *line = new QHBoxLayout(row);
        line->setContentsMargins(15, 10, 15, 10);
        row->setObjectName(run.connection.execution == runs_.mid(0, 2).last().connection.execution ? "lastRow" : "tableRow");
        row->setMinimumHeight(67);
        auto *identity = new QWidget;
        auto *names = column(identity, 0, 4);
        auto *name = label(run.shortName);
        name->setStyleSheet("font-weight:bold");
        names->addWidget(name);
        names->addWidget(label(run.period(), "small"));
        line->addWidget(identity, 42);
        line->addWidget(label(run.events), 18);
        auto *status = label(run.verified ? tr("✓  Завершён") : tr("Требуется сверка"), "small");
        if (run.verified) status->setStyleSheet("color:#16866b");
        line->addWidget(status, 26);
        auto *slot = new QWidget;
        auto *actions = new QHBoxLayout(slot);
        actions->setContentsMargins(0, 0, 0, 0);
        actions->addStretch();
        auto *open = button(tr("Открыть ›"), "link");
        actions->addWidget(open);
        line->addWidget(slot, 14);
        connect(open, &QPushButton::clicked, this, [this, run] { openRun(run, Catalog); });
        rows->addWidget(row);
    }
    layout->addWidget(table);
    layout->addStretch();
    return page;
}

QWidget *MainWindow::buildArchive()
{
    auto *page = new QWidget;
    page->setObjectName("surface");
    auto *layout = column(page);
    layout->addSpacing(7);
    heading(layout, tr("Анализ архива"), tr("Диагностика ранее накопленной телеметрии"));
    auto *card = panel();
    card->setMaximumWidth(750);
    auto *body = column(card, 20, 10);
    body->addWidget(sectionLabel(tr("Новый анализ Summit D2")));
    for (const auto &run : runs_) {
        if (!run.online()) {
            body->addWidget(label(tr("Доступный архив · %1").arg(run.period()), "muted"));
            break;
        }
    }
    body->addWidget(label(tr("Выберите дату, период и скорость воспроизведения."), "muted"));
    auto *start = button(tr("Новый запуск"), "primary");
    start->setEnabled(false);
    start->setToolTip(tr("Создание запуска подключается после визуальной приёмки"));
    body->addWidget(start, 0, Qt::AlignLeft);
    layout->addWidget(card);
    layout->addStretch();
    return page;
}

QWidget *MainWindow::buildCatalog()
{
    auto *page = new QWidget;
    page->setObjectName("surface");
    auto *layout = column(page);
    layout->addSpacing(7);
    heading(layout, tr("История запусков"), tr("Сохранённые результаты диагностики"));
    auto *filters = new QHBoxLayout;
    search_ = new QLineEdit;
    search_->setPlaceholderText(tr("Поиск запуска"));
    runFilter_ = new QComboBox;
    runFilter_->addItems({tr("Все запуски"), tr("Анализ архива"), tr("Имитация онлайн-мониторинга")});
    filters->addWidget(search_, 1);
    filters->addWidget(runFilter_);
    layout->addLayout(filters);
    auto *list = new QWidget;
    catalogList_ = column(list);
    layout->addWidget(list);
    layout->addStretch();
    connect(search_, &QLineEdit::textChanged, this, &MainWindow::populateCatalog);
    connect(runFilter_, &QComboBox::currentIndexChanged, this, &MainWindow::populateCatalog);
    populateCatalog();
    return page;
}

void MainWindow::populateCatalog()
{
    while (auto *item = catalogList_->takeAt(0)) {
        item->widget()->deleteLater();
        delete item;
    }
    for (const auto &run : runs_) {
        if (runFilter_->currentIndex() && (runFilter_->currentIndex() == 2) != run.online()) continue;
        if (!(run.name + " " + run.period() + " " + pace(run.speed)).contains(search_->text().trimmed(), Qt::CaseInsensitive)) continue;
        auto *card = panel();
        auto *layout = column(card, 19, 9);
        auto *titleRow = new QHBoxLayout;
        titleRow->addWidget(sectionLabel(run.name), 1);
        if (run.verified) {
            auto *status = label(tr("✓  Завершён"));
            status->setStyleSheet("color:#16866b");
            titleRow->addWidget(status);
        }
        layout->addLayout(titleRow);
        layout->addWidget(label(run.period(), "muted"));
        layout->addWidget(label(tr("%1  ·  %2  ·  %3 событий  ·  %4 результатов%5")
            .arg(run.online() ? tr("Имитация онлайн-мониторинга") : tr("Анализ архива"), pace(run.speed),
                 run.events, run.ticks, run.duration.isEmpty() ? QString{} : "  ·  " + run.duration), "small"));
        auto *open = button(tr("Открыть результаты ›"), "link");
        layout->addWidget(open, 0, Qt::AlignRight);
        connect(open, &QPushButton::clicked, this, [this, run] { openRun(run, Catalog); });
        catalogList_->addWidget(card);
    }
    if (!catalogList_->count()) catalogList_->addWidget(label(tr("Запуски не найдены"), "muted"));
}

QWidget *MainWindow::buildSettings()
{
    auto *page = new QWidget;
    page->setObjectName("surface");
    auto *layout = column(page);
    layout->addSpacing(7);
    heading(layout, tr("Настройки"), tr("Подключение к существующему запуску и параметры интерфейса"));
    auto *card = panel();
    card->setMaximumWidth(720);
    auto *form = column(card, 20);
    form->addWidget(sectionLabel(tr("Подключение к Master")));
    auto *setup = button(tr("Настроить подключение"), "primary");
    form->addWidget(setup, 0, Qt::AlignLeft);
    connect(setup, &QPushButton::clicked, this, [this] {
        SettingsDialog dialog(attached_.connection, this);
        if (dialog.exec() == QDialog::Accepted) applyConnection(dialog.settings());
    });
    layout->addWidget(card);
    layout->addStretch();
    return page;
}

QWidget *MainWindow::buildWorkspace()
{
    auto *page = new QWidget;
    auto *layout = column(page, 0, 0);
    breadcrumb_ = label({}, "small");
    layout->addWidget(breadcrumb_);
    layout->addSpacing(12);
    auto *header = new QHBoxLayout;
    auto *identity = new QWidget;
    auto *identityLayout = column(identity, 0, 8);
    runTitle_ = label({}, "workspaceTitle");
    runDescription_ = label({}, "muted");
    identityLayout->addWidget(runTitle_);
    identityLayout->addWidget(runDescription_);
    header->addWidget(identity, 1);
    runStatus_ = label({}, "small");
    header->addWidget(runStatus_);
    layout->addLayout(header);
    layout->addSpacing(4);
    runFacts_ = label({}, "small");
    runFacts_->setFixedHeight(28);
    runFacts_->setSizePolicy(QSizePolicy::Ignored, QSizePolicy::Fixed);
    layout->addWidget(runFacts_);
    layout->addSpacing(9);

    auto *bar = new QFrame;
    bar->setObjectName("tabs");
    auto *navigation = new QHBoxLayout(bar);
    navigation->setContentsMargins(4, 4, 4, 4);
    navigation->setSpacing(4);
    tabButtons_ = new QButtonGroup(this);
    tabs_ = new WorkspaceStack;
    tabs_->setObjectName("workspaceTabs");
    dashboard_ = new Dashboard;
    diagnostics_ = new Diagnostics;
    events_ = new Events;
    const QList<QWidget *> views{dashboard_, diagnostics_, events_};
    const QStringList names{tr("Обзор кластера"), tr("Диагностика группы"), tr("Журнал аномалий")};
    for (int i = 0; i < views.size(); ++i) {
        auto *item = button(names[i], "tab");
        item->setCheckable(true);
        item->setFixedHeight(35);
        tabButtons_->addButton(item, i);
        navigation->addWidget(item, 1);
        tabs_->addWidget(views[i]);
    }
    tabButtons_->button(0)->setChecked(true);
    layout->addWidget(bar);
    layout->addSpacing(10);
    auto *time = panel();
    time->setObjectName("workspaceTime");
    time->setFixedHeight(62);
    auto *controls = new QHBoxLayout(time);
    controls->setContentsMargins(12, 8, 12, 8);
    history_ = new HistoryBar;
    controls->addWidget(history_, 1);
    layout->addWidget(time);
    layout->addSpacing(12);
    body_ = new QScrollArea;
    body_->setObjectName("workspaceBody");
    body_->setFrameShape(QFrame::NoFrame);
    body_->setWidgetResizable(true);
    body_->setHorizontalScrollBarPolicy(Qt::ScrollBarAlwaysOff);
    body_->setVerticalScrollBarPolicy(Qt::ScrollBarAlwaysOn);
    body_->setWidget(tabs_);
    body_->viewport()->setObjectName("workspaceViewport");
    layout->addWidget(body_, 1);
    connect(tabButtons_, &QButtonGroup::idClicked, this, &MainWindow::showTab);
    connect(history_, &HistoryBar::changed, this, &MainWindow::refresh);
    connect(history_, &HistoryBar::followRequested, this, [this](bool following) {
        following_ = following;
        if (following_ && diagnostics_->interval() == 0)
            diagnostics_->setOptions(diagnostics_->criterion(), 1);
        if (!following_ && poller_.store().latestTime().isValid()) history_->selectTime(poller_.store().latestTime());
        refresh();
    });
    connect(dashboard_, &Dashboard::groupSelected, this, &MainWindow::selectGroup);
    connect(dashboard_, &Dashboard::historySelected, this, [this](const QString &group, const QDateTime &at) {
        openHistory(group, {}, {}, at);
    });
    connect(events_, &Events::eventSelected, this, [this](const QString &group, const QDateTime &from, const QDateTime &to) {
        diagnostics_->setOptions(diagnostics_->criterion(), 0);
        openHistory(group, from, to, from);
    });
    connect(dashboard_, &Dashboard::journalSelected, this, [this] { showTab(2); });
    connect(diagnostics_, &Diagnostics::timeSelected, this, [this](const QDateTime &at) {
        if (!following_) history_->selectTime(at);
        else openHistory({}, {}, {}, at);
    });
    connect(diagnostics_, &Diagnostics::selectionChanged, this, &MainWindow::refresh);
    connect(events_, &Events::filterChanged, this, &MainWindow::refresh);
    return page;
}

void MainWindow::navigate(Section section)
{
    section_ = section;
    navigation_->button(section)->setChecked(true);
    if (section == Online) {
        for (const auto &run : runs_) {
            if (run.online()) {
                openRun(run, Online);
                return;
            }
        }
        openRun(attached_, Online);
        return;
    }
    switch (section) {
    case Home: pages_->setCurrentIndex(HomePage); break;
    case Archive: pages_->setCurrentIndex(ArchivePage); break;
    case Catalog: pages_->setCurrentIndex(CatalogPage); break;
    case System: pages_->setCurrentIndex(SystemPage); break;
    case Settings: pages_->setCurrentIndex(SettingsPage); break;
    default: break;
    }
    refresh();
}

void MainWindow::openRun(const ReviewRun &run, Section section)
{
    const auto key = runKey(run);
    const bool different = !hasRun_ || key != runKey(current_);
    if (different && hasRun_) {
        auto previous = workspaceContext();
        if (!previous.at.isValid() && pendingTimeline_.at.isValid()) {
            previous.at = pendingTimeline_.at;
            previous.from = pendingTimeline_.from;
            previous.to = pendingTimeline_.to;
        }
        contexts_.insert(runKey(current_), previous);
    }
    current_ = run;
    hasRun_ = true;
    section_ = section;
    navigation_->button(section)->setChecked(true);
    pages_->setCurrentIndex(RunPage);
    if (different) {
        restoring_ = true;
        WorkspaceContext initial;
        initial.following = section == Online;
        const auto state = contexts_.value(key, initial);
        following_ = state.following;
        pendingTimeline_ = state;
        history_->setBounds({}, {});
        diagnostics_->selectGroup(state.group);
        diagnostics_->setOptions(state.criterion, state.interval);
        events_->restore(state.events);
        showTab(state.tab);
    }
    auto settings = run.connection;
    settings.intervalMs = attached_.connection.intervalMs;
    poller_.configure(settings);
    restoring_ = false;
    refresh();
}

MainWindow::WorkspaceContext MainWindow::workspaceContext() const
{
    return {diagnostics_->selectedGroup(), history_->at(), history_->from(), history_->to(),
            events_->context(), tabs_->currentIndex(), diagnostics_->criterion(),
            diagnostics_->interval(), following_};
}

void MainWindow::showTab(int index)
{
    tabButtons_->button(index)->setChecked(true);
    const bool changed = tabs_->currentIndex() != index;
    tabs_->setCurrentIndex(index);
    tabs_->updateGeometry();
    if (changed) body_->verticalScrollBar()->setValue(0);
    refresh();
}

void MainWindow::refresh()
{
    if (restoring_) return;
    updateConnection();
    if (pages_->currentIndex() == SystemPage) system_->refresh(poller_);
    if (pages_->currentIndex() != RunPage) return;
    runTitle_->setText(section_ == Online ? tr("Онлайн-мониторинг") : tr("Результаты анализа"));
    breadcrumb_->setText(section_ == Online ? tr("Главная  ›  Мониторинг") : tr("История запусков  ›  Результаты запуска"));
    runDescription_->setText(current_.shortName.isEmpty() ? current_.name : current_.shortName + " · " + current_.period());
    runStatus_->setText(current_.verified ? tr("✓  Завершён") : QString{});
    runStatus_->setStyleSheet("color:#16866b");
    QStringList facts;
    if (current_.online()) facts << tr("Имитация ×1 (архив Summit)");
    else if (!current_.speed.isEmpty() && current_.shortName.isEmpty()) facts << tr("Архив Summit · %1").arg(pace(current_.speed));
    if (!current_.ticks.isEmpty()) facts << tr("%1 результатов").arg(current_.ticks);
    if (!current_.events.isEmpty()) facts << tr("%1 событий").arg(current_.events);
    if (!current_.duration.isEmpty()) facts << current_.duration;
    const auto &store = poller_.store();
    if (current_.verified && !poller_.synchronized() && !poller_.stale())
        facts << tr("Загрузка результатов…");
    if (!current_.verified && store.summary.value("expected_ticks").toInteger() > 0) {
        facts << tr("%1 из %2 результатов")
            .arg(qMax(store.resultCount(), store.summary.value("group_ticks").toInteger()))
            .arg(store.summary.value("expected_ticks").toInteger());
    }
    runFacts_->setText(facts.join(" · "));
    updateHistory();
    const auto at = following_ ? store.latestTime() : history_->at();
    if (tabs_->currentIndex() == 0) dashboard_->refresh(store, at, !following_);
    else if (tabs_->currentIndex() == 1) {
        const int interval = diagnostics_->interval();
        const int seconds = interval == 2 ? 300 : 60;
        const bool eventWindow = interval == 0 && history_->from().isValid() && history_->to().isValid();
        const auto from = eventWindow ? history_->from() : qMax(store.earliestTime(), at.addSecs(-seconds));
        const auto to = eventWindow ? history_->to()
            : following_ ? at : qMin(store.latestTime(), at.addSecs(seconds));
        diagnostics_->refresh(store, at, from, to, current_.connection.execution);
    } else {
        const auto start = current_.start.isValid() ? current_.start : readTime(store.summary.value("window_start"));
        const auto end = current_.end.isValid() ? current_.end : readTime(store.summary.value("window_end"));
        const auto processedEnd = store.latestTime().addSecs(10);
        events_->refresh(store, start.isValid() ? start : store.earliestTime(),
                         end.isValid() && processedEnd.isValid() ? qMin(end, processedEnd) : processedEnd,
                         poller_.synchronized());
    }
}

void MainWindow::updateHistory()
{
    const auto &store = poller_.store();
    const auto start = current_.start.isValid() ? current_.start : readTime(store.summary.value("window_start"));
    const auto end = current_.end.isValid() ? current_.end : readTime(store.summary.value("window_end"));
    history_->setBounds(start.isValid() ? start : store.earliestTime(), end.isValid() ? end.addSecs(-10) : store.latestTime());
    restoreTimeline();
    if (!history_->hasSelection() && (current_.verified || store.latestTime().isValid())) {
        const QSignalBlocker blocker(history_);
        const auto latest = current_.verified && end.isValid() ? end.addSecs(-10) : store.latestTime();
        history_->setRange({}, {}, latest);
    }
    history_->setPresentation(!following_, section_ == Online || !current_.verified, store.latestTime());
}

void MainWindow::restoreTimeline()
{
    if (!pendingTimeline_.at.isValid()) return;
    const QSignalBlocker blocker(history_);
    history_->setRange(pendingTimeline_.from, pendingTimeline_.to, pendingTimeline_.at);
    history_->selectTime(pendingTimeline_.at);
    if (history_->hasSelection()) pendingTimeline_ = {};
}

void MainWindow::selectGroup(const QString &group)
{
    diagnostics_->selectGroup(group);
    showTab(1);
}

void MainWindow::openHistory(const QString &group, const QDateTime &from, const QDateTime &to, const QDateTime &at)
{
    const QSignalBlocker historyBlocker(history_);
    following_ = false;
    updateHistory();
    history_->setRange(from, to, at);
    if (group.isEmpty()) refresh(); else selectGroup(group);
}

void MainWindow::applyConnection(const ConnectionSettings &settings)
{
    if (attached_.connection.execution != settings.execution || attached_.connection.masterUrl != settings.masterUrl)
        attached_.speed.clear();
    attached_.connection = settings;
    preferences_.setValue("masterUrl", settings.masterUrl.toString());
    preferences_.setValue("execution", settings.execution);
    preferences_.setValue("intervalMs", settings.intervalMs);
    openRun(attached_, Online);
}

void MainWindow::updateConnection()
{
    const int health = poller_.health();
    const bool stale = poller_.stale();
    const auto &errors = poller_.errors();
    const bool summaryOnly = poller_.readiness() == 1 && errors.contains("/v1/summary")
        && !errors.contains("/v1/results") && !errors.contains("/v1/events");
    connection_->setText(health < 0 ? tr("●  Проверка Master")
        : health == 0 ? tr("●  Нет связи с Master")
        : summaryOnly ? tr("●  Сводка недоступна")
        : stale ? tr("●  Данные устарели") : tr("●  Master подключён"));
    connection_->setStyleSheet(health == 1 && !stale ? "color:#72d4b5" : "color:#c5d1df");
}
