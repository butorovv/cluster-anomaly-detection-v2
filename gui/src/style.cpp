#include "style.h"

#include <QPainter>
#include <QPainterPath>
#include <QResource>

static void initializeControls()
{
    Q_INIT_RESOURCE(v32controls);
}

QString v2::style()
{
    static const auto resources = [] { initializeControls(); return true; }();
    Q_UNUSED(resources);
    return QStringLiteral(R"(
        QWidget { color:#192b40; font-size:14px; }
        QMainWindow, QWidget#surface, QWidget#workspaceViewport, QStackedWidget#workspaceTabs, QScrollArea { background:#f3f5f9; }
        QWidget#sidebar { background:#18263a; }
        QWidget#sidebar QLabel { color:#c5d1df; font-size:12px; }
        QWidget#sidebar QLabel#brand { color:white; font-size:19px; font-weight:bold; }
        QWidget#sidebar QLabel#sideCaption { color:#9baec5; font-size:12px; }
        QWidget#sidebar QLabel#navHeading { color:#8fa3bb; font-size:11px; font-weight:bold; }
        QLabel#title { font-size:29px; font-weight:bold; }
        QLabel#workspaceTitle { font-size:25px; font-weight:bold; }
        QLabel#section { font-size:18px; font-weight:bold; }
        QLabel#tileTitle { font-size:20px; font-weight:bold; }
        QLabel#muted { color:#697b91; }
        QLabel#small { color:#697b91; font-size:12px; }
        QLabel#kpi { font-size:29px; font-weight:bold; }
        QFrame#panel, QFrame#workspaceTime, QTableView, QListWidget { background:white; border:1px solid #dce3ec; border-radius:10px; }
        QFrame#tableHead { background:#f7f9fc; border-bottom:1px solid #dce3ec; }
        QFrame#tableRow { border-bottom:1px solid #dce3ec; }
        QPushButton, QComboBox, QLineEdit, QDateTimeEdit, QSpinBox {
            min-height:28px; padding:4px 10px; background:white; border:1px solid #dce3ec; border-radius:7px;
        }
        QPushButton:hover { background:#f5f8fd; border-color:#a9c0e7; }
        QPushButton:focus, QComboBox:focus, QLineEdit:focus { border-color:#85a7e2; }
        QDateEdit, QTimeEdit { border:1px solid #dce3ec; border-radius:7px; background:white; padding:5px 8px; min-height:26px; font-size:13px; }
        QDateEdit:focus, QTimeEdit:focus { border-color:#85a7e2; }
        QComboBox::drop-down, QDateEdit::drop-down { subcontrol-origin:padding; subcontrol-position:top right; border:0; width:22px; }
        QComboBox, QDateEdit { padding-right:24px; }
        QComboBox::down-arrow { image:url(:/v32-controls/chevron.xpm); width:10px; height:7px; }
        QDateEdit::down-arrow { image:url(:/v32-controls/calendar.xpm); width:14px; height:14px; }
        QTimeEdit::up-button, QTimeEdit::down-button { subcontrol-origin:padding; border:0; width:18px; }
        QTimeEdit::up-button { subcontrol-position:top right; }
        QTimeEdit::down-button { subcontrol-position:bottom right; }
        QTimeEdit::up-arrow { image:url(:/v32-controls/up.xpm); width:8px; height:5px; }
        QTimeEdit::down-arrow { image:url(:/v32-controls/down.xpm); width:8px; height:5px; }
        QComboBox QAbstractItemView { border:1px solid #dce3ec; background:white; selection-background-color:#edf3fc; selection-color:#192b40; }
        QLabel#validationError { color:#c43648; font-size:12px; }
        QCalendarWidget#utcCalendar QWidget { background:white; }
        QCalendarWidget#utcCalendar QWidget#qt_calendar_navigationbar { background:#edf3fc; }
        QCalendarWidget#utcCalendar QToolButton { color:#295ec5; padding:5px; }
        QCalendarWidget#utcCalendar QAbstractItemView { selection-background-color:#295ec5; selection-color:white; }
        QPushButton:disabled { color:#8291a5; }
        QPushButton#primary { background:#295ec5; color:white; border-color:#295ec5; font-weight:bold; }
        QPushButton#primary:hover { background:#214fae; }
        QPushButton#primary:disabled { background:#5479c3; border-color:#5479c3; }
        QPushButton#soft { background:#edf3fc; color:#295ec5; border-color:#edf3fc; font-weight:bold; }
        QRadioButton#speedChoice { color:#192b40; background:white; border:1px solid #dce3ec; border-radius:7px; padding:3px 10px; spacing:8px; }
        QRadioButton#speedChoice:checked { background:#edf3fc; border-color:#85a7e2; color:#295ec5; }
        QRadioButton#speedChoice:focus { border-color:#85a7e2; }
        QWidget#modalShade { background-color:rgba(24,38,58,100); }
        QDialog#launchDialog { background:white; border:1px solid #dce3ec; }
        QPushButton#modalClose { min-width:30px; padding:0; font-size:19px; }
        QLabel#runNotice { background:#edf3fc; color:#405c7d; border-radius:7px; padding:9px 11px; }
        QPushButton#link { color:#295ec5; background:transparent; border:1px solid transparent; padding:0 2px; }
        QPushButton#link:focus { border-color:#85a7e2; }
        QPushButton#navButton { color:#c5d1df; background:transparent; border:1px solid transparent; padding:0 11px; min-height:41px; max-height:41px; text-align:left; font-size:13px; }
        QPushButton#navButton:hover { color:white; background:#253a51; }
        QPushButton#navButton:checked { color:white; background:#304b68; }
        QPushButton#navButton:focus { border-color:#8eb3f6; }
        QPushButton#criterion { text-align:left; font-weight:bold; min-height:74px; max-height:74px; padding:0; }
        QPushButton#criterion:checked { background:#edf3fc; border-color:#85a7e2; }
        QPushButton#groupButton { padding:0; }
        QPushButton#groupButton[anomaly="true"] { background:#fff0f1; border-color:#efc4ca; }
        QPushButton#nodeTile { font-size:12px; font-weight:bold; padding:3px 7px; }
        QPushButton#nodeTile[witness="true"] { color:#c43648; background:#fff0f1; border-color:#efc4ca; }
        QPushButton#routeBack { min-width:35px; max-width:35px; padding:0; font-size:18px; color:#295ec5; }
        QPushButton#routeLink { color:#295ec5; background:transparent; border-color:transparent; padding:0 3px; text-align:left; }
        QPushButton#routeLink:disabled, QLabel#routeLeaf { color:#192b40; font-weight:bold; }
        QPushButton#routeLink:focus { border-color:#85a7e2; }
        QPushButton#disclosure { text-align:left; font-weight:bold; background:transparent; border-color:transparent; padding:4px 0; }
        QPushButton#disclosure:focus, QPushButton#disclosure:hover { border-color:#85a7e2; }
        QPushButton#technicalDisclosure { text-align:left; font-weight:bold; }
        QPushButton#timeValue, QPushButton#timeValue:disabled {
            background:transparent; border:1px solid transparent; padding:0; min-height:15px;
            text-align:left; color:#192b40; font-size:15px; font-weight:bold;
        }
        QPushButton#timeValue:focus { border-color:#85a7e2; }
        QPushButton#timeStep { min-height:18px; padding:3px 8px; font-size:12px; }
        QHeaderView::section { background:#f7f9fc; color:#697b91; padding:10px 7px; border:0; border-bottom:1px solid #dce3ec; font-size:11px; font-weight:bold; }
        QTableView { selection-background-color:#edf3fc; selection-color:#192b40; gridline-color:#dce3ec; }
        QListWidget::item { padding:9px 13px; border-bottom:1px solid #dce3ec; }
        QProgressBar { border:0; border-radius:5px; background:#e3eaf5; text-align:center; }
        QProgressBar::chunk { background:#295ec5; border-radius:5px; }
        QSlider::groove:horizontal { height:6px; background:#e3eaf5; border-radius:3px; }
        QSlider::sub-page:horizontal { background:#295ec5; border-radius:3px; }
        QSlider::handle:horizontal { background:#295ec5; width:15px; margin:-5px 0; border-radius:7px; }
        QScrollBar:vertical { width:14px; background:transparent; margin:0; }
        QScrollBar::handle:vertical { background:#c8d2df; min-height:26px; border-radius:5px; margin:1px 2px; }
        QScrollBar::handle:vertical:disabled { background:transparent; }
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height:0; }
        QScrollBar::add-page:vertical, QScrollBar::sub-page:vertical { background:transparent; }
    )");
}

QIcon v2::icon(int index, const QColor &color, int size)
{
    QPixmap pixmap(size, size);
    pixmap.fill(Qt::transparent);
    QPainter p(&pixmap);
    p.setRenderHint(QPainter::Antialiasing);
    p.scale(size / 24., size / 24.);
    p.setPen(QPen(color, 1.65, Qt::SolidLine, Qt::RoundCap, Qt::RoundJoin));
    switch (index) {
    case 0: p.drawPolyline(QPolygonF(QList<QPointF>{{3,10},{12,3},{21,10},{21,20},{15,20},{15,14},{9,14},{9,20},{3,20},{3,10}})); break;
    case 1: p.drawPolyline(QPolygonF(QList<QPointF>{{2,12},{7,12},{11,3},{14,21},{18,12},{22,12}})); break;
    case 2:
        p.drawEllipse(QRectF(4,2,16,6));
        p.drawArc(QRectF(4,9,16,6), 0, -180*16);
        p.drawArc(QRectF(4,16,16,6), 0, -180*16);
        p.drawLine(4,5,4,19); p.drawLine(20,5,20,19); break;
    case 3:
        p.drawArc(QRectF(3,3,18,18), -200*16, 305*16);
        p.drawPolyline(QPolygonF(QList<QPointF>{{3,3},{3,8},{8,8}}));
        p.drawPolyline(QPolygonF(QList<QPointF>{{12,7},{12,12},{16,14}})); break;
    case 4:
        for (const int y : {3,13}) {
            p.drawRoundedRect(QRectF(3,y,18,8), 2, 2);
            p.drawPoint(7,y+4); p.drawLine(12,y+4,17,y+4);
        }
        break;
    case 6:
        p.drawPolyline(QPolygonF(QList<QPointF>{{15,5},{8,12},{15,19}}));
        break;
    case 7:
        p.drawPolyline(QPolygonF(QList<QPointF>{{9,5},{16,12},{9,19}}));
        break;
    default:
        p.drawEllipse(QRectF(5,5,14,14));
        p.drawEllipse(QRectF(9,9,6,6));
        for (int i = 0; i < 8; ++i) {
            p.save(); p.translate(12,12); p.rotate(45*i); p.drawLine(0,-7,0,-10); p.restore();
        }
    }
    return QIcon(pixmap);
}
