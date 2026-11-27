#pragma once

#include "api/poller.h"
#include <QWidget>

class QLabel;
class QProgressBar;
class QStandardItemModel;

class SystemView final : public QWidget {
public:
    explicit SystemView(QWidget *parent = nullptr);
    void refresh(const Poller &poller);
private:
    QLabel *health_;
    QLabel *progressText_;
    QLabel *clock_;
    QLabel *errors_;
    QLabel *metricsTime_;
    QProgressBar *progress_;
    QStandardItemModel *metricsModel_;
    QJsonObject lastMetrics_;
};
