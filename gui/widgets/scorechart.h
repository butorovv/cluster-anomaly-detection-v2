#pragma once

#include <QColor>
#include <QDateTime>
#include <QPointF>
#include <QWidget>

struct ChartSeries {
    QString name;
    QColor color;
    QList<QPointF> points;
};

class ScoreChart final : public QWidget {
    Q_OBJECT
public:
    explicit ScoreChart(QWidget *parent = nullptr);
    void setSeries(QList<ChartSeries> series, const QDateTime &from, const QDateTime &to,
                   const QDateTime &selected);
signals:
    void timeSelected(const QDateTime &time);
protected:
    void paintEvent(QPaintEvent *event) override;
    void mousePressEvent(QMouseEvent *event) override;
private:
    QRectF plotRect() const;
    QList<ChartSeries> series_;
    QDateTime from_;
    QDateTime to_;
    QDateTime selected_;
};
