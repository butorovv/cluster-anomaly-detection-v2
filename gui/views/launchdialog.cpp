#include "launchdialog.h"
#include "src/reviewrun.h"
#include "views/viewhelpers.h"
#include "widgets/utcrange.h"

#include <QButtonGroup>
#include <QComboBox>
#include <QGridLayout>
#include <QLineEdit>
#include <QPushButton>
#include <QRadioButton>
#include <QVBoxLayout>

LaunchDialog::LaunchDialog(bool online, QWidget *parent)
    : QDialog(parent), online_(online)
{
    const auto heading = online ? tr("Имитация онлайн-мониторинга") : tr("Новый запуск анализа");
    setWindowTitle(heading);
    setObjectName("launchDialog");
    setWindowFlag(Qt::FramelessWindowHint);
    setMinimumWidth(580);
    auto *layout = new QVBoxLayout(this);
    layout->setContentsMargins(23, 20, 23, 21);
    layout->setSpacing(10);
    auto *title = new QHBoxLayout;
    title->addWidget(sectionLabel(heading), 1);
    auto *close = new QPushButton(QStringLiteral("×"));
    close->setObjectName("modalClose");
    close->setAccessibleName(tr("Закрыть"));
    close->setAutoDefault(false);
    title->addWidget(close);
    layout->addLayout(title);
    connect(close, &QPushButton::clicked, this, &QDialog::reject);
    auto *caption = plainLabel(online ? tr("Подача архивной телеметрии в темпе ×1.")
                : tr("Выберите период телеметрии и скорость воспроизведения."));
    caption->setObjectName("muted");
    layout->addWidget(caption);
    layout->addSpacing(6);
    layout->addWidget(new QLabel(tr("Название запуска")));
    name_ = new QLineEdit(online ? tr("Summit — имитация мониторинга") : tr("Summit D2 — анализ архива"));
    name_->setMaxLength(120);
    name_->setAccessibleName(tr("Название запуска"));
    layout->addWidget(name_);
    layout->addWidget(new QLabel(tr("Источник телеметрии")));
    auto *source = new QComboBox;
    source->setAccessibleName(tr("Источник телеметрии"));
    source->addItem(tr("Summit D2 — архив телеметрии"));
    layout->addWidget(source);
    period_ = new UtcRange(tr("Начало (UTC)"), tr("Окончание (UTC)"));
    period_->setBounds(summitStart(), summitEnd());
    const auto shortStart = summitEnd().addSecs(-200);
    const auto shortEnd = shortStart.addSecs(140);
    period_->setPeriod(online ? shortStart : summitStart(), online ? shortEnd : summitEnd());
    layout->addWidget(period_);
    auto *available = plainLabel(tr("Доступный период: 18.01.2022, 08:00–16:00 UTC · шаг 10 с"));
    available->setObjectName("small");
    layout->addWidget(available);
    auto *quick = new QHBoxLayout;
    auto *whole = new QPushButton(tr("Весь архив"));
    auto *shortWindow = new QPushButton(tr("Короткий пример"));
    shortWindow->setObjectName("soft");
    whole->setAutoDefault(false);
    shortWindow->setAutoDefault(false);
    quick->addWidget(whole);
    quick->addWidget(shortWindow);
    quick->addStretch();
    layout->addLayout(quick);
    connect(whole, &QPushButton::clicked, this, [this] { period_->setPeriod(summitStart(), summitEnd()); });
    connect(shortWindow, &QPushButton::clicked, this, [this, shortStart, shortEnd] { period_->setPeriod(shortStart, shortEnd); });
    layout->addWidget(sectionLabel(tr("Скорость воспроизведения")));
    speeds_ = new QButtonGroup(this);
    if (online) {
        auto *notice = plainLabel(tr("×1 — один шаг телеметрии каждые 10 секунд"));
        notice->setObjectName("runNotice");
        layout->addWidget(notice);
    } else {
        auto *choicesGrid = new QGridLayout;
        const QStringList captions{tr("MAX\nБез задержек"), tr("×10\nШаг 1 секунда"),
                                   tr("×5\nШаг 2 секунды"), tr("×1\nШаг 10 секунд")};
        const QList<int> choices{0, 10, 5, 1};
        for (int i = 0; i < choices.size(); ++i) {
            auto *choice = new QRadioButton(captions[i]);
            choice->setObjectName("speedChoice");
            choice->setCheckable(true);
            choice->setMinimumHeight(54);
            speeds_->addButton(choice, choices[i]);
            choicesGrid->addWidget(choice, i / 2, i % 2);
        }
        speeds_->button(0)->setChecked(true);
        layout->addLayout(choicesGrid);
    }
    auto *notice = plainLabel(tr("Запуск создаёт изолированное окружение. Проверенные результаты сохраняются в истории."));
    notice->setObjectName("runNotice");
    layout->addWidget(notice);
    auto *actions = new QHBoxLayout;
    actions->addStretch();
    auto *cancel = new QPushButton(tr("Отмена"));
    auto *submit = new QPushButton(online ? tr("Запустить имитацию") : tr("Запустить анализ"));
    submit->setObjectName("primary");
    submit->setDefault(true);
    actions->addWidget(cancel);
    actions->addWidget(submit);
    layout->addLayout(actions);
    connect(cancel, &QPushButton::clicked, this, &QDialog::reject);
    connect(name_, &QLineEdit::textChanged, this, [this, submit] { submit->setEnabled(!name().isEmpty()); });
    connect(period_, &UtcRange::edited, this, [this] { period_->validate(10); });
    connect(submit, &QPushButton::clicked, this, [this] {
        if (!name().isEmpty() && period_->validate(10)) accept();
    });
    name_->selectAll();
    name_->setFocus();
}

QString LaunchDialog::name() const { return name_->text().trimmed(); }
QString LaunchDialog::speed() const { return online_ ? QStringLiteral("1") : QString::number(speeds_->checkedId()); }
QDateTime LaunchDialog::from() const { return period_->from(); }
QDateTime LaunchDialog::to() const { return period_->to(); }
