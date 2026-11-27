#pragma once

#include "api/masterclient.h"
#include <QDateTime>

struct ReviewRun {
    ConnectionSettings connection;
    QString name;
    QString shortName;
    QString speed;
    QDateTime start;
    QDateTime end;
    QString events;
    QString ticks;
    QString duration;
    bool verified = false;
    bool onlineSimulation = false;
    bool summitSource = false;
    QString directory;
    QString phase;
    QString phaseMessage;
    QDateTime created;
    bool snapshot = false;
    QString archiveMode;
    bool online() const { return onlineSimulation; }
    bool active() const;
    QString status() const;
    QString period() const;
};

QList<ReviewRun> readReviewRuns(const QString &root, int pollInterval);
QList<ReviewRun> readManagedRuns(const QString &root, int port, int pollInterval);
QDateTime summitStart();
QDateTime summitEnd();
