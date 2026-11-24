#include "mainwindow.h"

#include <QApplication>
#include <QStyleFactory>

int main(int argc, char *argv[])
{
    QApplication app(argc, argv);
    app.setOrganizationName("ClusterDiagnostics");
    app.setApplicationName("ClusterMonitor");
    app.setStyle(QStyleFactory::create("Fusion"));
    app.setStyleSheet(QStringLiteral(R"(
        QWidget { color:#182534; font-size:14px; }
        QMainWindow, QWidget#workspace { background:#f3f6fa; }
        QLabel#title { font-size:26px; font-weight:bold; }
        QLabel#section { font-size:17px; font-weight:bold; }
        QLabel#muted { color:#52647a; }
        QFrame#panel, QTableView, QListWidget { background:white; border:1px solid #dce4ee; border-radius:9px; }
        QPushButton, QComboBox, QLineEdit, QDateTimeEdit, QSpinBox {
            padding:6px 10px; background:white; border:1px solid #cad6e4; border-radius:6px;
        }
        QPushButton:hover { border-color:#376db1; background:#edf5ff; }
        QPushButton:checked { color:#144e91; background:#e2eeff; border-color:#abc8f2; font-weight:bold; }
        QPushButton:disabled { color:#778594; }
        QHeaderView::section { background:#eaf0f7; color:#334960; padding:7px; border:0; font-weight:bold; }
        QTableView { selection-background-color:#dbeaff; selection-color:#152537; gridline-color:#edf1f5; }
        QProgressBar { border:0; border-radius:5px; background:#e1e8f3; min-height:10px; text-align:center; }
        QProgressBar::chunk { background:#2b66ad; border-radius:5px; }
    )"));
    MainWindow window;
    window.show();
    return app.exec();
}
