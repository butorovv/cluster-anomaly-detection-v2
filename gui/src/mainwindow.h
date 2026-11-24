#pragma once

#include <QMainWindow>
#include <QSettings>
#include "api/poller.h"

class QLabel;
class QLineEdit;
class QStackedWidget;
class QButtonGroup;
class Dashboard;
class Diagnostics;
class Events;

class MainWindow : public QMainWindow {
    Q_OBJECT
public:
    explicit MainWindow(const ConnectionSettings &settings, QWidget *parent = nullptr);
private:
    void applyConnection(const ConnectionSettings &settings);
    void updateConnection();
    void refresh();
    void showPage(int index);
    QSettings preferences_;
    Poller poller_;
    QLabel *connection_;
    QLineEdit *execution_;
    QStackedWidget *pages_;
    QButtonGroup *navigation_;
    Dashboard *dashboard_;
    Diagnostics *diagnostics_;
    Events *events_;
    QTimer refreshTimer_;
};
