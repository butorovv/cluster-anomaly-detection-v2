#include "mainwindow.h"

#include <QButtonGroup>
#include <QDialog>
#include <QHBoxLayout>
#include <QLabel>
#include <QPushButton>
#include <QStackedWidget>
#include <QVBoxLayout>

MainWindow::MainWindow(QWidget *parent) : QMainWindow(parent)
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
        QDialog dialog(this);
        dialog.setWindowTitle(QStringLiteral("Настройки подключения"));
        dialog.exec();
    });
    layout->addWidget(pages_, 1);
}
