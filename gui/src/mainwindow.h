#pragma once

#include <QMainWindow>
#include <QSettings>
#include "api/poller.h"

class QLabel;
class QLineEdit;
class QStackedWidget;

class MainWindow : public QMainWindow {
    Q_OBJECT
public:
    explicit MainWindow(const ConnectionSettings &settings, QWidget *parent = nullptr);
private:
    void applyConnection(const ConnectionSettings &settings);
    void updateConnection();
    QSettings preferences_;
    Poller poller_;
    QLabel *connection_;
    QLineEdit *execution_;
    QStackedWidget *pages_;
};
