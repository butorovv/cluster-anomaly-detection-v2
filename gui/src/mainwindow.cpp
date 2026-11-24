#include "mainwindow.h"
#include "views/settings.h"

#include <QButtonGroup>
#include <QHBoxLayout>
#include <QLabel>
#include <QLineEdit>
#include <QPushButton>
#include <QStackedWidget>
#include <QVBoxLayout>

MainWindow::MainWindow(const ConnectionSettings &initial, QWidget *parent) : QMainWindow(parent)
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
    layout->addLayout(header);
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
    auto *navigation = new QHBoxLayout;
    layout->addLayout(navigation);
    auto *buttons = new QButtonGroup(this);
    pages_ = new QStackedWidget;
    const QStringList names{QStringLiteral("Обзор кластера"), QStringLiteral("Диагностика группы"),
                            QStringLiteral("Журнал аномалий"), QStringLiteral("Состояние системы")};
    for (const auto &name : names) {
        auto *button = new QPushButton(name);
        button->setCheckable(true);
        auto *page = new QWidget;
        auto *pageLayout = new QVBoxLayout(page);
        pageLayout->addWidget(new QLabel(name));
        pageLayout->addStretch();
        buttons->addButton(button, pages_->addWidget(page));
        navigation->addWidget(button);
    }
    buttons->button(0)->setChecked(true);
    connect(buttons, &QButtonGroup::idClicked, pages_, &QStackedWidget::setCurrentIndex);
    navigation->addStretch();
    auto *settings = new QPushButton(QStringLiteral("Настройки"));
    navigation->addWidget(settings);
    connect(settings, &QPushButton::clicked, this, [this] {
        SettingsDialog dialog(poller_.settings(), this);
        if (dialog.exec() == QDialog::Accepted) applyConnection(dialog.settings());
    });
    layout->addWidget(pages_, 1);
    connect(&poller_, &Poller::updated, this, &MainWindow::updateConnection);
    connect(&poller_, &Poller::reset, this, &MainWindow::updateConnection);
    applyConnection(initial);
}

void MainWindow::applyConnection(const ConnectionSettings &settings)
{
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
}
