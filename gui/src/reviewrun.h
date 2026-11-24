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
    bool online() const { return speed == "1"; }
    QString period() const;
};

QList<ReviewRun> readReviewRuns(const QString &root, int pollInterval);
