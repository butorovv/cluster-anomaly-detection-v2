#pragma once

#include "models/monitorstore.h"
#include <QWidget>

class QGridLayout;
class QLabel;
class EventTable;
class QTableView;
class QPushButton;

class Dashboard final : public QWidget {
    Q_OBJECT
public:
    explicit Dashboard(QWidget *parent = nullptr);
    void refresh(const MonitorStore &store, const QDateTime &at);
    void resetEvents() { eventCount_ = -1; }

signals:
    void groupSelected(const QString &group);
    void eventSelected(const QString &group, const QDateTime &at, const QDateTime &end);
    void journalSelected();

private:
    struct GroupCard {
        QPushButton *button;
        QLabel *dot;
        QLabel *votes;
        QLabel *status;
    };
    void rebuildCards(const QStringList &groups);
    QGridLayout *grid_;
    QMap<QString, GroupCard> cards_;
    QList<QLabel *> counts_;
    EventTable *recent_;
    QPushButton *journal_;
    qsizetype eventCount_ = -1;
    const MonitorStore *eventStore_ = nullptr;
};
