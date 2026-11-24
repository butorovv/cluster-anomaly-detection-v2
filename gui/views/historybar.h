#pragma once

#include <QDateTime>
#include <QWidget>

class QLabel;
class QPushButton;
class QSlider;
class QStackedWidget;

class HistoryBar final : public QWidget {
    Q_OBJECT
public:
    explicit HistoryBar(QWidget *parent = nullptr);
    void setBounds(const QDateTime &start, const QDateTime &end);
    void setRange(const QDateTime &from, const QDateTime &to, const QDateTime &at);
    void selectTime(const QDateTime &at);
    void setPresentation(bool history, bool canFollow, const QDateTime &latest);
    bool hasSelection() const { return at_.isValid(); }
    QDateTime from() const { return from_; }
    QDateTime to() const { return to_; }
    QDateTime at() const { return at_; }

signals:
    void changed();
    void followRequested(bool following);

private:
    QDateTime normalized(const QDateTime &value) const;
    void render();
    void openTimePicker();
    QLabel *caption_;
    QPushButton *selectedTime_;
    QSlider *slider_;
    QLabel *first_;
    QLabel *middle_;
    QLabel *last_;
    QStackedWidget *range_;
    QPushButton *previous_;
    QPushButton *next_;
    QPushButton *follow_;
    QDateTime minimum_;
    QDateTime maximum_;
    QDateTime at_;
    QDateTime from_;
    QDateTime to_;
    QDateTime latest_;
    bool historical_ = true;
    bool canFollow_ = false;
};
