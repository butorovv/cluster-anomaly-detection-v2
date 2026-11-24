#pragma once

#include "models/monitorstore.h"
#include <QWidget>

class QComboBox;
class QLabel;
class NodeTable;
class ScoreChart;

class Diagnostics final : public QWidget {
    Q_OBJECT
public:
    explicit Diagnostics(QWidget *parent = nullptr);
    void selectGroup(const QString &group);
    void refresh(const MonitorStore &store, const QDateTime &at, const QDateTime &from, const QDateTime &to);

signals:
    void selectionChanged();
    void timeSelected(const QDateTime &time);

private:
    void displayTick(const GroupTick *tick, const QDateTime &at);
    void refreshChart(const MonitorStore &store, const QDateTime &at, const QDateTime &from, const QDateTime &to);

    QComboBox *groups_;
    QComboBox *metric_;
    QLabel *current_;
    QLabel *raw_;
    QLabel *temporal_;
    QLabel *coverage_;
    QLabel *outside_;
    QLabel *evidence_;
    NodeTable *nodes_;
    ScoreChart *chart_;
};
