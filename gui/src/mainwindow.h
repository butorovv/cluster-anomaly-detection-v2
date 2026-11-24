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
class NodeView;
class QPushButton;
class SystemView;

class MainWindow : public QMainWindow {
    Q_OBJECT
public:
    explicit MainWindow(const ConnectionSettings &settings, const QString &summitSpeed = {},
                        QList<ReviewRun> runs = {}, QWidget *parent = nullptr);
    void configurePhysicalArchive(const QString &assets);
    void configureManagedRuns(const QString &root, const QString &powershell);
private:
    enum Section { Home, Online, Archive, Catalog, System, Settings };
    enum Page { HomePage, RunPage, ArchivePage, CatalogPage, SystemPage, SettingsPage };
    enum Level { Overview, Group, Journal, Node };
    struct Route {
        int level;
        QString group;
        int scroll;
    };
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
        QString node;
        int nodeInterval = 1;
        QList<Route> trail;
    };
    void navigate(Section section);
    void openRun(const ReviewRun &run, Section section);
    void applyConnection(const ConnectionSettings &settings);
    void updateConnection();
    void refresh();
    void showTab(int index);
    void enter(int index);
    void back();
    void showJournal(const QString &group = {});
    void updateRoute();
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
    void refreshManagedRuns();
    void launchRun(bool online);
    void connectRun(const ReviewRun &run);

    QSettings preferences_;
    Poller poller_;
    QList<ReviewRun> runs_;
    QList<ReviewRun> importedRuns_;
    ReviewRun attached_;
    ReviewRun current_;
    QMap<QString, WorkspaceContext> contexts_;
    WorkspaceContext pendingTimeline_;
    bool hasRun_ = false;
    bool restoring_ = false;
    bool following_ = false;
    bool submitting_ = false;
    QString assets_, managedRoot_, powershell_, catalogVersion_;
    static constexpr int managedPort_ = 18086;
    Section section_ = Home;
    QLabel *connection_;
    QLabel *runTitle_;
    QLabel *runDescription_;
    QLabel *runFacts_;
    QLabel *runStatus_;
    QPushButton *startOnline_;
    QPushButton *resumeArchive_;
    QLabel *breadcrumb_;
    QLineEdit *search_;
    QComboBox *runFilter_;
    QVBoxLayout *catalogList_;
    QStackedWidget *pages_;
    QStackedWidget *tabs_;
    QScrollArea *body_;
    QButtonGroup *navigation_;
    QPushButton *back_;
    QPushButton *clusterCrumb_;
    QPushButton *groupCrumb_;
    QLabel *leafCrumb_;
    QList<Route> trail_;
    Dashboard *dashboard_;
    Diagnostics *diagnostics_;
    Events *events_;
    NodeView *node_;
    HistoryBar *history_;
    SystemView *system_;
    QTimer refreshTimer_;
    QTimer managedTimer_;
};
