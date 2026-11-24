#pragma once

#include <QMainWindow>

class QStackedWidget;

class MainWindow : public QMainWindow {
    Q_OBJECT
public:
    explicit MainWindow(QWidget *parent = nullptr);
private:
    QStackedWidget *pages_;
};
