#pragma once

#include "models/eventtable.h"
#include <QWidget>

class QComboBox;
class QLabel;
class QPushButton;
class QSortFilterProxyModel;
class QTableView;

struct EventViewContext {
    QString group;
    QString event;
    QDateTime from;
    QDateTime to;
    int sortColumn = 1;
    Qt::SortOrder sortOrder = Qt::DescendingOrder;
};

class Events final : public QWidget {
    Q_OBJECT
public:
    explicit Events(QWidget *parent = nullptr);
    void refresh(const MonitorStore &store, const QDateTime &start, const QDateTime &end);
    EventViewContext context() const;
    void restore(const EventViewContext &state);

signals:
    void filterChanged();
    void eventSelected(const QString &group, const QDateTime &detected, const QDateTime &closed);

private:
    void showDetails();
    void editPeriod();
    const AnomalyEvent *selectedEvent() const;
    QComboBox *group_;
    QTableView *table_;
    QLabel *count_;
    QLabel *periodLabel_;
    QLabel *title_;
    QLabel *detected_;
    QLabel *closed_;
    QLabel *duration_;
    QLabel *votes_;
    QLabel *technical_;
    QPushButton *open_;
    QPushButton *technicalToggle_;
    QPushButton *period_;
    EventTable *model_;
    QSortFilterProxyModel *sort_;
    EventViewContext state_;
    qsizetype previousCount_ = -1;
    QString previousGroup_;
    QDateTime previousFrom_;
    QDateTime previousTo_;
    QDateTime minimum_;
    QDateTime maximum_;
};
