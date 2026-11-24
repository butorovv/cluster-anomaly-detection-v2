#pragma once

#include <QMainWindow>
#include <QSettings>
#include "api/poller.h"

class QLabel;
class QLineEdit;
class QStackedWidget;
class QButtonGroup;
class QComboBox;
class Dashboard;
class Diagnostics;
class Events;
class HistoryBar;
class SystemView;

class MainWindow : public QMainWindow {
    Q_OBJECT
public:
    explicit MainWindow(const ConnectionSettings &settings, const QString &summitSpeed = {}, QWidget *parent = nullptr);
private:
    enum Mode { Live, FastReplay, History };
    void applyConnection(const ConnectionSettings &settings);
    void updateConnection();
    void refresh();
    void showPage(int index);
    void selectGroup(const QString &group);
    void openHistory(const QString &group, const QDateTime &from, const QDateTime &to, const QDateTime &at);
    void updateHistory();
    QSettings preferences_;
    Poller poller_;
    QLabel *connection_;
    QLineEdit *execution_;
    QComboBox *mode_;
    QLabel *source_;
    QStackedWidget *pages_;
    QButtonGroup *navigation_;
    Dashboard *dashboard_;
    Diagnostics *diagnostics_;
    Events *events_;
    HistoryBar *history_;
    SystemView *system_;
    QTimer refreshTimer_;
    QString summitSpeed_;
};
