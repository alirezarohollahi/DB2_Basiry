# Operational Database Tables

## 1. centers

جدول مراکز آموزشی / درمانی.

| Field | Description |
|---|---|
| id | شناسه مرکز |
| name | نام مرکز |
| city | شهر |
| address | آدرس مرکز |
| is_active | فعال یا غیرفعال بودن مرکز |
| created_at | زمان ایجاد |
| updated_at | زمان آخرین ویرایش |

---

## 2. children

جدول کودکان / دانش‌آموزان.

| Field | Description |
|---|---|
| id | شناسه کودک |
| center_id | شناسه مرکز |
| first_name | نام |
| last_name | نام خانوادگی |
| national_code | کد ملی |
| birth_date | تاریخ تولد |
| gender | جنسیت |
| enrollment_date | تاریخ ثبت‌نام |
| status | وضعیت کودک |
| created_at | زمان ایجاد |
| updated_at | زمان آخرین ویرایش |

---

## 3. teachers

جدول معلمان.

| Field | Description |
|---|---|
| id | شناسه معلم |
| center_id | شناسه مرکز |
| first_name | نام |
| last_name | نام خانوادگی |
| phone | شماره تماس |
| email | ایمیل |
| employment_status | وضعیت همکاری |
| is_active | فعال یا غیرفعال بودن معلم |
| created_at | زمان ایجاد |
| updated_at | زمان آخرین ویرایش |

---

## 4. users

جدول کاربران سیستم.

| Field | Description |
|---|---|
| id | شناسه کاربر |
| username | نام کاربری |
| password_hash | رمز عبور هش‌شده |
| role | نقش کاربر |
| teacher_id | شناسه معلم، در صورت معلم بودن کاربر |
| is_active | فعال یا غیرفعال بودن کاربر |
| created_at | زمان ایجاد |
| updated_at | زمان آخرین ویرایش |

---

## 5. domains

جدول حوزه‌های سنجش.

| Field | Description |
|---|---|
| id | شناسه حوزه |
| name | نام حوزه |
| description | توضیحات حوزه |
| is_active | فعال یا غیرفعال بودن حوزه |
| created_at | زمان ایجاد |
| updated_at | زمان آخرین ویرایش |

---

## 6. task_templates

جدول تعریف پایه‌ای تسک‌ها.

| Field | Description |
|---|---|
| id | شناسه تسک پایه |
| domain_id | شناسه حوزه |
| title | عنوان تسک |
| description | توضیحات تسک |
| default_score_scale_id | مقیاس نمره‌دهی پیش‌فرض |
| is_active | فعال یا غیرفعال بودن تسک |
| created_by | ایجادکننده تسک |
| created_at | زمان ایجاد |
| updated_at | زمان آخرین ویرایش |

---

## 7. score_scales

جدول مقیاس‌های نمره‌دهی.

| Field | Description |
|---|---|
| id | شناسه مقیاس نمره‌دهی |
| name | نام مقیاس |
| min_score | حداقل نمره |
| max_score | حداکثر نمره |
| description | توضیحات |
| is_active | فعال یا غیرفعال بودن |
| created_at | زمان ایجاد |
| updated_at | زمان آخرین ویرایش |

---

## 8. center_daily_status

جدول وضعیت روزانه مرکز.

| Field | Description |
|---|---|
| id | شناسه رکورد |
| center_id | شناسه مرکز |
| date | تاریخ |
| status | وضعیت مرکز در آن روز |
| closure_reason_id | دلیل تعطیلی، در صورت تعطیل بودن |
| note | توضیح مربوط به وضعیت مرکز |
| created_by | ثبت‌کننده |
| created_at | زمان ایجاد |
| updated_at | زمان آخرین ویرایش |

---

## 9. closure_reasons

جدول دلایل تعطیلی مرکز.

| Field | Description |
|---|---|
| id | شناسه دلیل تعطیلی |
| title | عنوان دلیل |
| description | توضیحات |
| is_active | فعال یا غیرفعال بودن |
| created_at | زمان ایجاد |
| updated_at | زمان آخرین ویرایش |

---

## 10. child_daily_status

جدول وضعیت روزانه کودک.

| Field | Description |
|---|---|
| id | شناسه رکورد |
| child_id | شناسه کودک |
| date | تاریخ |
| status | وضعیت کودک در آن روز |
| absence_reason_id | دلیل غیبت، در صورت غیبت |
| note | توضیح مربوط به وضعیت کودک در آن روز |
| created_by | ثبت‌کننده |
| created_at | زمان ایجاد |
| updated_at | زمان آخرین ویرایش |

---

## 11. absence_reasons

جدول دلایل غیبت کودک.

| Field | Description |
|---|---|
| id | شناسه دلیل غیبت |
| title | عنوان دلیل |
| description | توضیحات |
| is_active | فعال یا غیرفعال بودن |
| created_at | زمان ایجاد |
| updated_at | زمان آخرین ویرایش |

---

## 12. child_task_plans

جدول برنامه تسک‌های کودک در بازه زمانی.

| Field | Description |
|---|---|
| id | شناسه برنامه تسک |
| child_id | شناسه کودک |
| task_template_id | شناسه تسک پایه، در صورت وجود |
| domain_id | شناسه حوزه |
| task_title | عنوان واقعی تسک برای کودک |
| score_scale_id | شناسه مقیاس نمره‌دهی |
| start_date | تاریخ شروع برنامه |
| end_date | تاریخ پایان برنامه |
| is_active | فعال یا غیرفعال بودن برنامه |
| created_by | ایجادکننده برنامه |
| created_at | زمان ایجاد |
| updated_at | زمان آخرین ویرایش |

---

## 13. daily_task_assignments

جدول تسک‌های تعریف‌شده برای کودک در هر روز.

| Field | Description |
|---|---|
| id | شناسه تسک روزانه |
| child_id | شناسه کودک |
| date | تاریخ |
| child_task_plan_id | شناسه برنامه تسک |
| task_template_id | شناسه تسک پایه، در صورت وجود |
| domain_id | شناسه حوزه |
| task_title | عنوان تسک در همان روز |
| score_scale_id | شناسه مقیاس نمره‌دهی |
| planned_by | کاربری که تسک را برنامه‌ریزی کرده |
| status | وضعیت تسک روزانه |
| created_at | زمان ایجاد |
| updated_at | زمان آخرین ویرایش |

---

## 14. assessment_sessions

جدول جلسات سنجش معلم با کودک.

| Field | Description |
|---|---|
| id | شناسه جلسه |
| child_id | شناسه کودک |
| teacher_id | شناسه معلم |
| center_id | شناسه مرکز |
| date | تاریخ جلسه |
| started_at | زمان شروع جلسه |
| ended_at | زمان پایان جلسه |
| session_status | وضعیت جلسه |
| general_note | توضیح کلی جلسه |
| created_at | زمان ایجاد |
| updated_at | زمان آخرین ویرایش |

---

## 15. task_assessments

جدول نتیجه سنجش هر تسک.

| Field | Description |
|---|---|
| id | شناسه نتیجه سنجش |
| daily_task_assignment_id | شناسه تسک روزانه |
| assessment_session_id | شناسه جلسه سنجش |
| child_id | شناسه کودک |
| teacher_id | شناسه معلم |
| date | تاریخ سنجش |
| score | نمره خام |
| normalized_score | نمره نرمال‌شده |
| assessment_status | وضعیت سنجش |
| no_score_reason_id | دلیل ثبت نشدن نمره |
| attempt_no | شماره تلاش |
| note | توضیح مربوط به سنجش |
| created_at | زمان ایجاد |
| updated_at | زمان آخرین ویرایش |

---

## 16. no_score_reasons

جدول دلایل ثبت نشدن نمره.

| Field | Description |
|---|---|
| id | شناسه دلیل |
| title | عنوان دلیل |
| description | توضیحات |
| is_active | فعال یا غیرفعال بودن |
| created_at | زمان ایجاد |
| updated_at | زمان آخرین ویرایش |

---

## 17. notes

جدول توضیحات در سطوح مختلف.

| Field | Description |
|---|---|
| id | شناسه Note |
| note_scope | سطح توضیح |
| center_id | شناسه مرکز، در صورت نیاز |
| child_id | شناسه کودک، در صورت نیاز |
| teacher_id | شناسه معلم، در صورت نیاز |
| date | تاریخ مربوط به Note |
| daily_task_assignment_id | شناسه تسک روزانه، در صورت نیاز |
| task_assessment_id | شناسه نتیجه سنجش، در صورت نیاز |
| note_text | متن توضیح |
| created_by | ایجادکننده Note |
| created_at | زمان ایجاد |
| updated_at | زمان آخرین ویرایش |

---

## 18. note_batches

جدول ثبت گروهی توضیحات.

| Field | Description |
|---|---|
| id | شناسه Batch |
| created_by | ایجادکننده Batch |
| note_scope | سطح توضیحات ثبت‌شده |
| note_text | متن مشترک توضیحات |
| created_at | زمان ایجاد |

---

## 19. note_batch_items

جدول اتصال Noteهای ثبت‌شده به Batch.

| Field | Description |
|---|---|
| id | شناسه رکورد |
| note_batch_id | شناسه Batch |
| note_id | شناسه Note |

---

## 20. audit_logs

جدول لاگ تغییرات مهم سیستم.

| Field | Description |
|---|---|
| id | شناسه لاگ |
| user_id | کاربر انجام‌دهنده تغییر |
| entity_name | نام جدول یا موجودیت |
| entity_id | شناسه رکورد تغییرکرده |
| action | نوع عملیات |
| old_value | مقدار قبلی |
| new_value | مقدار جدید |
| created_at | زمان ثبت تغییر |