#pragma once

#include "api/poller.h"
#include "reviewrun.h"
#include "views/events.h"
#include <QMainWindow>
#include <QSettings>

class QLabel;
class QStackedWidget;
class QButtonGroup;
class QComboBox;
class QLineEdit;
class QScrollArea;
class QVBoxLayout;
class Dashboard;
class Diagnostics;
class HistoryBar;
class SystemView;

class MainWindow : public QMainWindow {
    Q_OBJECT
public:
    explicit MainWindow(const ConnectionSettings &settings, const QString &summitSpeed = {},
                        QList<ReviewRun> runs = {}, QWidget *parent = nullptr);
private:
    enum Section { Home, Online, Archive, Catalog, System, Settings };
    enum Page { HomePage, RunPage, ArchivePage, CatalogPage, SystemPage, SettingsPage };
    struct WorkspaceContext {
        QString group;
        QDateTime at;
        QDateTime from;
        QDateTime to;
        EventViewContext events;
        int tab = 0;
        int criterion = 0;
        int interval = 1;
        bool following = false;
    };
    void navigate(Section section);
    void openRun(const ReviewRun &run, Section section);
    void applyConnection(const ConnectionSettings &settings);
    void updateConnection();
    void refresh();
    void showTab(int index);
    void selectGroup(const QString &group);
    void openHistory(const QString &group, const QDateTime &from, const QDateTime &to, const QDateTime &at);
    void updateHistory();
    void restoreTimeline();
    WorkspaceContext workspaceContext() const;
    QWidget *buildHome();
    QWidget *buildArchive();
    QWidget *buildCatalog();
    QWidget *buildSettings();
    QWidget *buildWorkspace();
    void populateCatalog();

    QSettings preferences_;
    Poller poller_;
    QList<ReviewRun> runs_;
    ReviewRun attached_;
    ReviewRun current_;
    QMap<QString, WorkspaceContext> contexts_;
    WorkspaceContext pendingTimeline_;
    bool hasRun_ = false;
    bool restoring_ = false;
    bool following_ = false;
    Section section_ = Home;
    QLabel *connection_;
    QLabel *runTitle_;
    QLabel *runDescription_;
    QLabel *runFacts_;
    QLabel *runStatus_;
    QLabel *breadcrumb_;
    QLineEdit *search_;
    QComboBox *runFilter_;
    QVBoxLayout *catalogList_;
    QStackedWidget *pages_;
    QStackedWidget *tabs_;
    QScrollArea *body_;
    QButtonGroup *navigation_;
    QButtonGroup *tabButtons_;
    Dashboard *dashboard_;
    Diagnostics *diagnostics_;
    Events *events_;
    HistoryBar *history_;
    SystemView *system_;
    QTimer refreshTimer_;
};
