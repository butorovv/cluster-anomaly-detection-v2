#pragma once

#include "models/monitorstore.h"
#include <QWidget>

class QComboBox;
class QLabel;
class QPushButton;
class QTableView;
class QGridLayout;
class QResizeEvent;
class NodeTable;
class ScoreChart;

class Diagnostics final : public QWidget {
    Q_OBJECT
public:
    explicit Diagnostics(QWidget *parent = nullptr);
    void selectGroup(const QString &group);
    void setOptions(int criterion, int interval);
    QString selectedGroup() const { return selectedGroup_; }
    int criterion() const { return criterion_; }
    int interval() const;
    QStringList hosts() const { return hosts_; }
    void refresh(const MonitorStore &store, const QDateTime &at, const QDateTime &from, const QDateTime &to,
                 const QString &execution = {});

signals:
    void selectionChanged();
    void timeSelected(const QDateTime &time);
    void nodeSelected(const QString &node);
    void journalSelected(const QString &group);

private:
    void resizeEvent(QResizeEvent *event) override;
    void showTopology(const GroupTick *tick, const GroupTick *inventory);
    void arrangeNodes();
    void displayTick(const GroupTick *tick, const QString &execution);
    void refreshChart(const MonitorStore &store, const QDateTime &at, const QDateTime &from, const QDateTime &to);
    void chooseCriterion(int index);

    QComboBox *groups_;
    QComboBox *interval_;
    QLabel *current_;
    QLabel *state_;
    QLabel *votes_;
    QLabel *chartTitle_;
    QLabel *explanation_;
    QLabel *details_;
    QLabel *outside_;
    QLabel *evidence_;
    QTableView *table_;
    NodeTable *nodes_;
    ScoreChart *chart_;
    QLabel *detailHeading_;
    QLabel *topologyCaption_;
    QGridLayout *topology_;
    QList<QPushButton *> nodeButtons_;
    QStringList hosts_;
    int columns_ = 0;
    QWidget *detailBody_;
    QWidget *localBlock_;
    QList<QPushButton *> criteria_;
    QList<QLabel *> criterionStates_;
    QString selectedGroup_;
    int criterion_ = 0;
};
