#pragma once

#include <QDateTime>
#include <QWidget>

class QDateTimeEdit;

class HistoryBar final : public QWidget {
    Q_OBJECT
public:
    explicit HistoryBar(QWidget *parent = nullptr);
    void setBounds(const QDateTime &start, const QDateTime &end);
    void setRange(const QDateTime &from, const QDateTime &to, const QDateTime &at);
    void selectTime(const QDateTime &at);
    bool hasSelection() const;
    QDateTime from() const;
    QDateTime to() const;
    QDateTime at() const;

signals:
    void changed();

private:
    void apply(const QDateTime &from, const QDateTime &to, const QDateTime &at);
    QDateTime normalized(const QDateTime &value) const;

    QDateTimeEdit *from_;
    QDateTimeEdit *to_;
    QDateTimeEdit *at_;
    QDateTime minimum_;
    QDateTime maximum_;
    bool selected_ = false;
};
