#pragma once

#include "api/masterclient.h"
#include <QWidget>

class QLabel;
class QLineEdit;
class QSpinBox;
class QPushButton;

class SettingsForm final : public QWidget {
    Q_OBJECT
public:
    explicit SettingsForm(const ConnectionSettings &settings, QWidget *parent = nullptr);
    ConnectionSettings settings() const;
signals:
    void submitted(const ConnectionSettings &connection, bool attach);

private:
    bool validate();
    void check();
    void finishCheck(const QString &message);

    QLineEdit *url_;
    QLineEdit *execution_;
    QSpinBox *interval_;
    QLabel *status_;
    QPushButton *check_;
    MasterClient client_;
};
