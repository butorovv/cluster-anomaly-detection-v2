#pragma once

#include "models/monitorstore.h"
#include <QWidget>

class QGridLayout;
class QLabel;
class QListWidget;
class QPushButton;
class QProgressBar;

class Dashboard final : public QWidget {
    Q_OBJECT
public:
    explicit Dashboard(QWidget *parent = nullptr);
    void refresh(const MonitorStore &store, const QDateTime &at);

signals:
    void groupSelected(const QString &group);
    void historySelected(const QString &group, const QDateTime &at);

private:
    void rebuildCards(const QStringList &groups);
    QGridLayout *grid_;
    QMap<QString, QPushButton *> cards_;
    QList<QLabel *> counts_;
    QListWidget *recent_;
    QLabel *time_;
    QProgressBar *progress_;
};
