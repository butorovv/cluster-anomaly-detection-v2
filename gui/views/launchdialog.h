#pragma once

#include <QDateTime>
#include <QDialog>

class QButtonGroup;
class QLineEdit;
class UtcRange;

class LaunchDialog final : public QDialog {
    Q_OBJECT
public:
    explicit LaunchDialog(bool online, QWidget *parent = nullptr);
    QString name() const;
    QString speed() const;
    QDateTime from() const;
    QDateTime to() const;
private:
    UtcRange *period_;
    QLineEdit *name_;
    QButtonGroup *speeds_;
    bool online_;
};
