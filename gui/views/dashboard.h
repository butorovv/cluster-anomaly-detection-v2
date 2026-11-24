#pragma once

#include "models/monitorstore.h"
#include <QWidget>

class QGridLayout;
class QLabel;
class QListWidget;
class QPushButton;

class Dashboard final : public QWidget {
    Q_OBJECT
public:
    explicit Dashboard(QWidget *parent = nullptr);
    void refresh(const MonitorStore &store, const QDateTime &at, bool historical = false);

signals:
    void groupSelected(const QString &group);
    void historySelected(const QString &group, const QDateTime &at);
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
    QListWidget *recent_;
    QLabel *recentTitle_;
    bool historical_ = false;
};
