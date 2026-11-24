#pragma once

#include "api/masterclient.h"
#include <QDialog>

class QLabel;
class QLineEdit;
class QSpinBox;
class QPushButton;

class SettingsDialog final : public QDialog {
public:
    explicit SettingsDialog(const ConnectionSettings &settings, QWidget *parent);
    ConnectionSettings settings() const;

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
