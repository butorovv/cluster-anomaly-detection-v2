#pragma once

#include <QDateTime>
#include <QWidget>

class QDateEdit;
class QTimeEdit;
class QLabel;

class UtcRange final : public QWidget {
    Q_OBJECT
public:
    UtcRange(const QString &startLabel, const QString &endLabel, QWidget *parent = nullptr);
    QDateTime from() const;
    QDateTime to() const;
    void setPeriod(const QDateTime &from, const QDateTime &to, bool preserveEditing = false);
    void setBounds(const QDateTime &minimum, const QDateTime &maximum);
    bool validate(int stepSeconds = 1);

signals:
    void edited();

private:
    QDateEdit *startDate_;
    QDateEdit *endDate_;
    QTimeEdit *startTime_;
    QTimeEdit *endTime_;
    QLabel *error_;
    QDateTime minimum_;
    QDateTime maximum_;
};
