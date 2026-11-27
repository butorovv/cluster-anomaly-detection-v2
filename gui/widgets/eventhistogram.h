#pragma once

#include <QDateTime>
#include <QWidget>

class EventHistogram final : public QWidget {
    Q_OBJECT
public:
    explicit EventHistogram(QWidget *parent = nullptr);
    void setData(const QList<QDateTime> &starts, const QDateTime &minimum, const QDateTime &maximum,
                 const QDateTime &from, const QDateTime &to);
signals:
    void intervalSelected(const QDateTime &from, const QDateTime &to);
protected:
    void paintEvent(QPaintEvent *) override;
    void mouseReleaseEvent(QMouseEvent *event) override;
    void keyPressEvent(QKeyEvent *event) override;
    bool event(QEvent *event) override;
private:
    int binAt(qreal x) const;
    void selectBin(int index);
    QList<int> counts_;
    QDateTime base_, minimum_, maximum_, from_, to_;
    int seconds_ = 3600;
    int cursor_ = -1;
};
