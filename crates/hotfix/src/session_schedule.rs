use crate::config::ScheduleConfig;
use crate::session::error::SessionCreationError;
use chrono::{DateTime, Datelike, Days, NaiveDate, NaiveTime, TimeDelta, Timelike, Utc, Weekday};
use chrono_tz::Tz;
use thiserror::Error;

/// Result of comparing two times to determine if they fall within the same session period.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionPeriodComparison {
    SamePeriod,
    DifferentPeriod,
    OutsideSessionTime { which: WhichTime },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WhichTime {
    First,
    Second,
    Both,
}

#[derive(Debug, Error)]
pub enum ScheduleError {
    #[error("ambiguous or missing time: {date} {time} in timezone {timezone} (DST transition)")]
    AmbiguousOrMissingTime {
        date: NaiveDate,
        time: NaiveTime,
        timezone: Tz,
    },

    #[error("date calculation overflow: {context}")]
    DateCalculationOverflow { context: String },
}

#[derive(Clone, Debug)]
pub enum SessionSchedule {
    NonStop,
    Daily {
        start_time: NaiveTime,
        end_time: NaiveTime,
        timezone: Tz,
    },
    Weekdays {
        start_time: NaiveTime,
        end_time: NaiveTime,
        weekdays: Vec<Weekday>,
        timezone: Tz,
    },
    Weekly {
        start_day: Weekday,
        start_time: NaiveTime,
        end_day: Weekday,
        end_time: NaiveTime,
        timezone: Tz,
    },
}

impl SessionSchedule {
    pub fn is_active_at(&self, datetime: &DateTime<Utc>) -> bool {
        match self {
            SessionSchedule::NonStop => true,
            SessionSchedule::Daily {
                start_time,
                end_time,
                timezone,
            } => {
                let adjusted_datetime = datetime.with_timezone(timezone);
                Self::check_daily_schedule(&adjusted_datetime, start_time, end_time)
            }
            SessionSchedule::Weekdays {
                weekdays,
                start_time,
                end_time,
                timezone,
            } => {
                let adjusted_datetime = datetime.with_timezone(timezone);
                Self::check_weekdays_schedule(&adjusted_datetime, weekdays, start_time, end_time)
            }
            SessionSchedule::Weekly {
                start_day,
                start_time,
                end_day,
                end_time,
                timezone,
            } => {
                let adjusted_datetime = datetime.with_timezone(timezone);
                Self::check_weekly_schedule(
                    &adjusted_datetime,
                    start_day,
                    start_time,
                    end_day,
                    end_time,
                )
            }
        }
    }

    pub fn is_same_session_period(
        &self,
        dt1: &DateTime<Utc>,
        dt2: &DateTime<Utc>,
    ) -> Result<SessionPeriodComparison, ScheduleError> {
        let dt1_active = self.is_active_at(dt1);
        let dt2_active = self.is_active_at(dt2);

        if !dt1_active || !dt2_active {
            let which = match (dt1_active, dt2_active) {
                (false, false) => WhichTime::Both,
                (false, true) => WhichTime::First,
                (true, false) => WhichTime::Second,
                (true, true) => unreachable!(),
            };
            return Ok(SessionPeriodComparison::OutsideSessionTime { which });
        }

        let (start, end) = self.get_session_bounds(dt1)?;
        if start <= *dt2 && *dt2 < end {
            Ok(SessionPeriodComparison::SamePeriod)
        } else {
            Ok(SessionPeriodComparison::DifferentPeriod)
        }
    }

    fn get_session_bounds(
        &self,
        datetime: &DateTime<Utc>,
    ) -> Result<(DateTime<Utc>, DateTime<Utc>), ScheduleError> {
        match self {
            SessionSchedule::NonStop => {
                Ok((DateTime::default(), Utc::now() + TimeDelta::weeks(1000)))
            }
            SessionSchedule::Daily {
                start_time,
                end_time,
                timezone,
            } => calculate_single_day_session_bounds(datetime, start_time, end_time, timezone),
            SessionSchedule::Weekdays {
                start_time,
                end_time,
                timezone,
                weekdays: _,
            } => calculate_single_day_session_bounds(datetime, start_time, end_time, timezone),
            SessionSchedule::Weekly {
                start_day,
                start_time,
                end_day,
                end_time,
                timezone,
            } => calculate_weekly_session_bounds(
                datetime, start_day, start_time, end_day, end_time, timezone,
            ),
        }
    }

    fn check_daily_schedule(
        datetime: &DateTime<Tz>,
        start_time: &NaiveTime,
        end_time: &NaiveTime,
    ) -> bool {
        if start_time < end_time {
            &datetime.time() >= start_time && &datetime.time() < end_time
        } else {
            &datetime.time() >= start_time || &datetime.time() < end_time
        }
    }

    fn check_weekdays_schedule(
        datetime: &DateTime<Tz>,
        weekdays: &[Weekday],
        start_time: &NaiveTime,
        end_time: &NaiveTime,
    ) -> bool {
        let time_of_day = &datetime.time();

        if start_time < end_time {
            // schedule doesn't span midnight
            weekdays.contains(&datetime.weekday())
                && time_of_day >= start_time
                && time_of_day < end_time
        } else {
            // schedule spans midnight
            if time_of_day >= end_time && time_of_day < start_time {
                return false;
            }

            let target_day = if time_of_day >= start_time {
                datetime.weekday()
            } else {
                datetime.weekday().pred()
            };
            weekdays.contains(&target_day)
        }
    }

    fn check_weekly_schedule(
        datetime: &DateTime<Tz>,
        start_day: &Weekday,
        start_time: &NaiveTime,
        end_day: &Weekday,
        end_time: &NaiveTime,
    ) -> bool {
        let start_pos = weekly_seconds(start_day, start_time);
        let end_pos = weekly_seconds(end_day, end_time);
        let now_pos = weekly_seconds(&datetime.weekday(), &datetime.time());

        if start_pos < end_pos {
            // e.g., Mon 09:00 → Fri 17:00
            start_pos <= now_pos && now_pos < end_pos
        } else {
            // e.g., Sun 18:00 → Fri 17:00
            now_pos >= start_pos || now_pos < end_pos
        }
    }
}

impl TryFrom<&ScheduleConfig> for SessionSchedule {
    type Error = SessionCreationError;

    fn try_from(config: &ScheduleConfig) -> Result<Self, Self::Error> {
        match config {
            // NonStop: no configuration provided
            ScheduleConfig {
                start_time: None,
                end_time: None,
                start_day: None,
                end_day: None,
                weekdays,
                timezone: None,
            } if weekdays.is_empty() => Ok(SessionSchedule::NonStop),

            // Daily/weekdays sessions
            ScheduleConfig {
                start_time: Some(start),
                end_time: Some(end),
                start_day: None,
                end_day: None,
                weekdays,
                timezone,
            } => {
                if weekdays.is_empty() {
                    if start == end {
                        Ok(SessionSchedule::NonStop)
                    } else {
                        Ok(SessionSchedule::Daily {
                            start_time: *start,
                            end_time: *end,
                            timezone: timezone.unwrap_or(Tz::UTC),
                        })
                    }
                } else if start == end {
                    Err(SessionCreationError::InvalidSchedule(
                        "Start and end times cannot be equal when weekdays is set".to_string(),
                    ))
                } else {
                    Ok(SessionSchedule::Weekdays {
                        start_time: *start,
                        end_time: *end,
                        weekdays: weekdays.clone(),
                        timezone: timezone.unwrap_or(Tz::UTC),
                    })
                }
            }

            // Weekly sessions
            ScheduleConfig {
                start_day: Some(start_day),
                start_time: Some(start),
                end_day: Some(end_day),
                end_time: Some(end),
                weekdays,
                timezone,
            } => {
                // Weekdays should be empty for weekly sessions
                if !weekdays.is_empty() {
                    return Err(SessionCreationError::InvalidSchedule(
                        "weekly sessions cannot have weekdays specified".to_string(),
                    ));
                }

                if start_day == end_day && start < end {
                    return Err(SessionCreationError::InvalidSchedule(
                        "Incorrect weekly schedule: start time must be after end time for same day weekly schedule".to_string(),
                    ));
                }

                if start_day == end_day && start == end {
                    return Ok(SessionSchedule::NonStop);
                }

                Ok(SessionSchedule::Weekly {
                    start_day: *start_day,
                    start_time: *start,
                    end_day: *end_day,
                    end_time: *end,
                    timezone: timezone.unwrap_or(Tz::UTC),
                })
            }

            // Invalid combinations
            _ => Err(SessionCreationError::InvalidSchedule(
                "invalid schedule configuration: incomplete or conflicting parameters".to_string(),
            )),
        }
    }
}

impl TryFrom<Option<&ScheduleConfig>> for SessionSchedule {
    type Error = SessionCreationError;

    fn try_from(maybe_schedule: Option<&ScheduleConfig>) -> Result<Self, Self::Error> {
        match maybe_schedule {
            None => Ok(SessionSchedule::NonStop),
            Some(session_config) => session_config.try_into(),
        }
    }
}

/// Linear coordinate of a (weekday, time) pair as seconds since Monday 00:00.
fn weekly_seconds(day: &Weekday, time: &NaiveTime) -> u32 {
    day.num_days_from_monday() * 86_400 + time.num_seconds_from_midnight()
}

fn construct_utc(
    date: &NaiveDate,
    time: &NaiveTime,
    timezone: &Tz,
) -> Result<DateTime<Utc>, ScheduleError> {
    if let Some(dt) = date.and_time(*time).and_local_timezone(*timezone).single() {
        Ok(dt.to_utc())
    } else {
        Err(ScheduleError::AmbiguousOrMissingTime {
            date: *date,
            time: *time,
            timezone: *timezone,
        })
    }
}

fn calculate_single_day_session_bounds(
    datetime: &DateTime<Utc>,
    start_time: &NaiveTime,
    end_time: &NaiveTime,
    timezone: &Tz,
) -> Result<(DateTime<Utc>, DateTime<Utc>), ScheduleError> {
    let local_datetime = datetime.with_timezone(timezone);

    if local_datetime.time() >= *start_time {
        // if the datetime is greater than the start_time, they fall on the same day
        let start = construct_utc(&local_datetime.date_naive(), start_time, timezone)?;

        // if the end_time is smaller than the start_time, then it must be the next day
        let end_date = if end_time < start_time {
            local_datetime
                .date_naive()
                .checked_add_days(Days::new(1))
                .ok_or_else(|| ScheduleError::DateCalculationOverflow {
                    context: "failed to add day for end date".to_string(),
                })?
        } else {
            local_datetime.date_naive()
        };
        let end = construct_utc(&end_date, end_time, timezone)?;
        Ok((start, end))
    } else {
        // if the datetime is lesser than the start_time, it must fall on the previous day
        let start_date = local_datetime
            .date_naive()
            .checked_sub_days(Days::new(1))
            .ok_or_else(|| ScheduleError::DateCalculationOverflow {
                context: "failed to get previous day for start date".to_string(),
            })?;
        let start = construct_utc(&start_date, start_time, timezone)?;
        let end = construct_utc(&local_datetime.date_naive(), end_time, timezone)?;
        Ok((start, end))
    }
}

fn calculate_weekly_session_bounds(
    datetime: &DateTime<Utc>,
    start_day: &Weekday,
    start_time: &NaiveTime,
    end_day: &Weekday,
    end_time: &NaiveTime,
    timezone: &Tz,
) -> Result<(DateTime<Utc>, DateTime<Utc>), ScheduleError> {
    let local_datetime = datetime.with_timezone(timezone);

    let days_back: u64 = {
        let curr = local_datetime.weekday().num_days_from_monday() as i64;
        let start = start_day.num_days_from_monday() as i64;
        let diff = (curr - start).rem_euclid(7) as u64;

        if diff == 0 && local_datetime.time() < *start_time {
            7 // i.e. Monday 9 AM -> Monday 3 AM
        } else {
            diff
        }
    };

    let session_start_date = local_datetime
        .date_naive()
        .checked_sub_days(Days::new(days_back))
        .ok_or_else(|| ScheduleError::DateCalculationOverflow {
            context: "failed to compute weekly session start date".to_string(),
        })?;

    let session_duration: u64 = {
        let s = start_day.num_days_from_monday() as i64;
        let e = end_day.num_days_from_monday() as i64;
        let diff = (e - s).rem_euclid(7) as u64;
        if diff == 0 && end_time <= start_time {
            7
        } else {
            diff
        }
    };

    let end_date = session_start_date
        .checked_add_days(Days::new(session_duration))
        .ok_or_else(|| ScheduleError::DateCalculationOverflow {
            context: "failed to compute weekly session end date".to_string(),
        })?;

    let start = construct_utc(&session_start_date, start_time, timezone)?;
    let end = construct_utc(&end_date, end_time, timezone)?;
    Ok((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveTime, Weekday};

    fn utc_dt(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<Utc> {
        DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(y, m, d)
                .unwrap()
                .and_hms_opt(h, mi, s)
                .unwrap(),
            Utc,
        )
    }

    #[test]
    fn test_active_at_non_stop_schedule() {
        // non-stop schedules are always active
        let schedule = SessionSchedule::NonStop;
        assert!(schedule.is_active_at(&Utc::now()))
    }

    #[test]
    fn test_active_at_daily_schedule_utc() {
        let schedule = SessionSchedule::Daily {
            start_time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
            end_time: NaiveTime::from_hms_opt(17, 0, 0).unwrap(),
            timezone: Tz::UTC,
        };

        // just before start time (8:59:59)
        let before_start = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_opt(8, 59, 59)
                .unwrap(),
            Utc,
        );
        assert!(!schedule.is_active_at(&before_start));

        // just after start time (9:00:01)
        let after_start = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_opt(9, 0, 1)
                .unwrap(),
            Utc,
        );
        assert!(schedule.is_active_at(&after_start));

        // in the middle (13:00:00)
        let middle = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_opt(13, 0, 0)
                .unwrap(),
            Utc,
        );
        assert!(schedule.is_active_at(&middle));

        // just before end time (16:59:59)
        let before_end = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_opt(16, 59, 59)
                .unwrap(),
            Utc,
        );
        assert!(schedule.is_active_at(&before_end));

        // at end time (17:00:00) - we expect false at exactly the end time (non-inclusive)
        let at_end = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_opt(17, 0, 0)
                .unwrap(),
            Utc,
        );
        assert!(!schedule.is_active_at(&at_end));

        // after end time (17:00:01)
        let after_end = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_opt(17, 0, 1)
                .unwrap(),
            Utc,
        );
        assert!(!schedule.is_active_at(&after_end));
    }

    #[test]
    fn test_active_at_daily_schedule_london() {
        // we'll use 27/06/2025 as the date
        // London is an hour ahead of UTC
        let schedule = SessionSchedule::Daily {
            start_time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
            end_time: NaiveTime::from_hms_opt(17, 0, 0).unwrap(),
            timezone: Tz::Europe__London,
        };

        let before_start = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 6, 27)
                .unwrap()
                .and_hms_opt(7, 59, 59)
                .unwrap(),
            Utc,
        );
        assert!(!schedule.is_active_at(&before_start));

        // 8AM UTC is 9AM London time, so already in session
        let at_start = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 6, 27)
                .unwrap()
                .and_hms_opt(8, 0, 0)
                .unwrap(),
            Utc,
        );
        assert!(schedule.is_active_at(&at_start));

        // 4PM UTC is 5PM London time, so already out of session
        let at_end = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 6, 27)
                .unwrap()
                .and_hms_opt(16, 0, 0)
                .unwrap(),
            Utc,
        );
        assert!(!schedule.is_active_at(&at_end));
    }

    #[test]
    fn test_active_at_daily_schedule_london_end_before_start() {
        // we'll use 27/06/2025 as the date
        // London is an hour ahead of UTC
        let schedule_1 = SessionSchedule::Daily {
            start_time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
            end_time: NaiveTime::from_hms_opt(2, 0, 0).unwrap(),
            timezone: Tz::Europe__London,
        };

        let before_start = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 6, 27)
                .unwrap()
                .and_hms_opt(7, 59, 59)
                .unwrap(),
            Utc,
        );
        assert!(!schedule_1.is_active_at(&before_start));

        let at_start = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 6, 27)
                .unwrap()
                .and_hms_opt(8, 0, 0)
                .unwrap(),
            Utc,
        );
        assert!(schedule_1.is_active_at(&at_start));

        let before_end = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 6, 27)
                .unwrap()
                .and_hms_opt(0, 59, 59)
                .unwrap(),
            Utc,
        );
        assert!(schedule_1.is_active_at(&before_end));

        let at_end = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 6, 27)
                .unwrap()
                .and_hms_opt(1, 0, 0)
                .unwrap(),
            Utc,
        );
        assert!(!schedule_1.is_active_at(&at_end));
    }

    #[test]
    fn test_active_at_daily_schedule_london_end_before_start_tz_crossing_midnight() {
        // we'll use 27/06/2025 as the date
        // London is an hour ahead of UTC
        let schedule_1 = SessionSchedule::Daily {
            start_time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
            end_time: NaiveTime::from_hms_opt(0, 30, 0).unwrap(),
            timezone: Tz::Europe__London,
        };

        let before_start = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 6, 27)
                .unwrap()
                .and_hms_opt(7, 59, 59)
                .unwrap(),
            Utc,
        );
        assert!(!schedule_1.is_active_at(&before_start));

        let at_start = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 6, 27)
                .unwrap()
                .and_hms_opt(8, 0, 0)
                .unwrap(),
            Utc,
        );
        assert!(schedule_1.is_active_at(&at_start));

        let before_end = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 6, 27)
                .unwrap()
                .and_hms_opt(23, 29, 59)
                .unwrap(),
            Utc,
        );
        assert!(schedule_1.is_active_at(&before_end));

        let at_end = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 6, 27)
                .unwrap()
                .and_hms_opt(23, 30, 0)
                .unwrap(),
            Utc,
        );
        assert!(!schedule_1.is_active_at(&at_end));
    }

    #[test]
    fn test_active_at_weekdays_schedule_utc() {
        // Monday to Friday, 9AM to 5PM UTC
        let schedule = SessionSchedule::Weekdays {
            start_time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
            end_time: NaiveTime::from_hms_opt(17, 0, 0).unwrap(),
            weekdays: vec![
                Weekday::Mon,
                Weekday::Tue,
                Weekday::Wed,
                Weekday::Thu,
                Weekday::Fri,
            ],
            timezone: Tz::UTC,
        };

        // Monday 8:59:59 - before start time
        let monday_before_start = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 6, 30) // Monday
                .unwrap()
                .and_hms_opt(8, 59, 59)
                .unwrap(),
            Utc,
        );
        assert!(!schedule.is_active_at(&monday_before_start));

        // Monday 9:00:01 - after start time
        let monday_after_start = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 6, 30) // Monday
                .unwrap()
                .and_hms_opt(9, 0, 1)
                .unwrap(),
            Utc,
        );
        assert!(schedule.is_active_at(&monday_after_start));

        // Friday 16:59:59 - just before end time on a valid day
        let friday_before_end = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 7, 4) // Friday
                .unwrap()
                .and_hms_opt(16, 59, 59)
                .unwrap(),
            Utc,
        );
        assert!(schedule.is_active_at(&friday_before_end));

        // Friday 17:00:00 - at end time on a valid day (exclusive end)
        let friday_at_end = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 7, 4) // Friday
                .unwrap()
                .and_hms_opt(17, 0, 0)
                .unwrap(),
            Utc,
        );
        assert!(!schedule.is_active_at(&friday_at_end));

        // Saturday 12:00:00 - middle of day on weekend
        let saturday = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 7, 5) // Saturday
                .unwrap()
                .and_hms_opt(12, 0, 0)
                .unwrap(),
            Utc,
        );
        assert!(!schedule.is_active_at(&saturday));
    }

    #[test]
    fn test_active_at_weekdays_schedule_london() {
        // Monday to Friday, 9AM to 5PM London time
        // During summer (June), London is UTC+1
        let schedule = SessionSchedule::Weekdays {
            start_time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
            end_time: NaiveTime::from_hms_opt(17, 0, 0).unwrap(),
            weekdays: vec![
                Weekday::Mon,
                Weekday::Tue,
                Weekday::Wed,
                Weekday::Thu,
                Weekday::Fri,
            ],
            timezone: Tz::Europe__London,
        };

        // Monday 7:59:59 UTC = 8:59:59 London - before start time
        let monday_before_start = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 6, 30) // Monday
                .unwrap()
                .and_hms_opt(7, 59, 59)
                .unwrap(),
            Utc,
        );
        assert!(!schedule.is_active_at(&monday_before_start));

        // Monday 8:00:01 UTC = 9:00:01 London - after start time
        let monday_after_start = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 6, 30) // Monday
                .unwrap()
                .and_hms_opt(8, 0, 1)
                .unwrap(),
            Utc,
        );
        assert!(schedule.is_active_at(&monday_after_start));

        // Friday 15:59:59 UTC = 16:59:59 London - just before end time
        let friday_before_end = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 7, 4) // Friday
                .unwrap()
                .and_hms_opt(15, 59, 59)
                .unwrap(),
            Utc,
        );
        assert!(schedule.is_active_at(&friday_before_end));

        // Friday 16:00:00 UTC = 17:00:00 London - at end time (exclusive)
        let friday_at_end = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 7, 4) // Friday
                .unwrap()
                .and_hms_opt(16, 0, 0)
                .unwrap(),
            Utc,
        );
        assert!(!schedule.is_active_at(&friday_at_end));
    }

    #[test]
    fn test_active_at_weekdays_schedule_newyork() {
        // Monday to Friday, 9:30AM to 4PM New York time
        // During summer (June), New York is UTC-4
        let schedule = SessionSchedule::Weekdays {
            start_time: NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
            end_time: NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
            weekdays: vec![
                Weekday::Mon,
                Weekday::Tue,
                Weekday::Wed,
                Weekday::Thu,
                Weekday::Fri,
            ],
            timezone: Tz::America__New_York,
        };

        // Monday 13:29:59 UTC = 9:29:59 NY - just before start time
        let monday_before_start = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 6, 30) // Monday
                .unwrap()
                .and_hms_opt(13, 29, 59)
                .unwrap(),
            Utc,
        );
        assert!(!schedule.is_active_at(&monday_before_start));

        // Monday 13:30:01 UTC = 9:30:01 NY - just after start time
        let monday_after_start = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 6, 30) // Monday
                .unwrap()
                .and_hms_opt(13, 30, 1)
                .unwrap(),
            Utc,
        );
        assert!(schedule.is_active_at(&monday_after_start));

        // Tuesday 19:59:59 UTC = 15:59:59 NY - just before end time
        let tuesday_before_end = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 7, 1) // Tuesday
                .unwrap()
                .and_hms_opt(19, 59, 59)
                .unwrap(),
            Utc,
        );
        assert!(schedule.is_active_at(&tuesday_before_end));

        // Tuesday 20:00:00 UTC = 16:00:00 NY - at end time (exclusive)
        let tuesday_at_end = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 7, 1) // Tuesday
                .unwrap()
                .and_hms_opt(20, 0, 0)
                .unwrap(),
            Utc,
        );
        assert!(!schedule.is_active_at(&tuesday_at_end));
    }

    #[test]
    fn test_active_at_weekdays_schedule_sydney_crossing_midnight() {
        // Monday to Friday, 10PM to 6AM Sydney time (crosses midnight)
        // In June, Sydney is UTC+10
        let schedule = SessionSchedule::Weekdays {
            start_time: NaiveTime::from_hms_opt(22, 0, 0).unwrap(),
            end_time: NaiveTime::from_hms_opt(6, 0, 0).unwrap(),
            weekdays: vec![
                Weekday::Mon,
                Weekday::Tue,
                Weekday::Wed,
                Weekday::Thu,
                Weekday::Fri,
            ],
            timezone: Tz::Australia__Sydney,
        };

        // Monday 11:59:59 UTC = 21:59:59 Sydney - just before start time
        let monday_before_start = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 6, 30) // Monday
                .unwrap()
                .and_hms_opt(11, 59, 59)
                .unwrap(),
            Utc,
        );
        assert!(!schedule.is_active_at(&monday_before_start));

        // Monday 12:00:01 UTC = 22:00:01 Sydney - just after start time
        let monday_after_start = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 6, 30) // Monday
                .unwrap()
                .and_hms_opt(12, 0, 1)
                .unwrap(),
            Utc,
        );
        assert!(schedule.is_active_at(&monday_after_start));

        // Tuesday 19:59:59 UTC = 5:59:59 Sydney Wednesday - just before end time
        let tuesday_before_end = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 7, 1) // Tuesday
                .unwrap()
                .and_hms_opt(19, 59, 59)
                .unwrap(),
            Utc,
        );
        assert!(schedule.is_active_at(&tuesday_before_end));

        // Tuesday 20:00:00 UTC = 6:00:00 Sydney Wednesday - at end time (exclusive)
        let tuesday_at_end = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 7, 1) // Tuesday
                .unwrap()
                .and_hms_opt(20, 0, 0)
                .unwrap(),
            Utc,
        );
        assert!(!schedule.is_active_at(&tuesday_at_end));

        // Wednesday 10:00:00 UTC = 20:00:00 Sydney - during inactive period
        let wednesday_inactive = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 7, 2) // Wednesday
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            Utc,
        );
        assert!(!schedule.is_active_at(&wednesday_inactive));
    }

    #[test]
    fn test_active_at_weekdays_schedule_only_weekend() {
        // Weekend schedule (Saturday and Sunday), 10AM to 4PM UTC
        let schedule = SessionSchedule::Weekdays {
            start_time: NaiveTime::from_hms_opt(10, 0, 0).unwrap(),
            end_time: NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
            weekdays: vec![Weekday::Sat, Weekday::Sun],
            timezone: Tz::UTC,
        };

        // Friday 12:00:00 - should be inactive (weekday)
        let friday = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 7, 4) // Friday
                .unwrap()
                .and_hms_opt(12, 0, 0)
                .unwrap(),
            Utc,
        );
        assert!(!schedule.is_active_at(&friday));

        // Saturday 9:59:59 - before start time on weekend
        let saturday_before_start = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 7, 5) // Saturday
                .unwrap()
                .and_hms_opt(9, 59, 59)
                .unwrap(),
            Utc,
        );
        assert!(!schedule.is_active_at(&saturday_before_start));

        // Saturday 10:00:01 - after start time on weekend
        let saturday_after_start = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 7, 5) // Saturday
                .unwrap()
                .and_hms_opt(10, 0, 1)
                .unwrap(),
            Utc,
        );
        assert!(schedule.is_active_at(&saturday_after_start));

        // Sunday 15:59:59 - before end time on weekend
        let sunday_before_end = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 7, 6) // Sunday
                .unwrap()
                .and_hms_opt(15, 59, 59)
                .unwrap(),
            Utc,
        );
        assert!(schedule.is_active_at(&sunday_before_end));
    }

    #[test]
    fn test_active_at_weekdays_schedule_overnight_crossing_weekdays() {
        // Monday to Thursday nights, 10PM to 6AM London time
        // During summer (June), London is UTC+1
        let schedule = SessionSchedule::Weekdays {
            start_time: NaiveTime::from_hms_opt(22, 0, 0).unwrap(),
            end_time: NaiveTime::from_hms_opt(6, 0, 0).unwrap(),
            weekdays: vec![Weekday::Mon, Weekday::Tue, Weekday::Wed, Weekday::Thu],
            timezone: Tz::Europe__London,
        };

        // Monday 20:59:59 UTC = 21:59:59 London - just before start time
        let monday_before_start = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 6, 30) // Monday
                .unwrap()
                .and_hms_opt(20, 59, 59)
                .unwrap(),
            Utc,
        );
        assert!(!schedule.is_active_at(&monday_before_start));

        // Monday 21:00:01 UTC = 22:00:01 London - just after start time
        let monday_after_start = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 6, 30) // Monday
                .unwrap()
                .and_hms_opt(21, 0, 1)
                .unwrap(),
            Utc,
        );
        assert!(schedule.is_active_at(&monday_after_start));

        // Tuesday 4:59:59 UTC = 5:59:59 London - just before end time, still Monday's session
        let tuesday_before_end = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 7, 1) // Tuesday
                .unwrap()
                .and_hms_opt(4, 59, 59)
                .unwrap(),
            Utc,
        );
        assert!(schedule.is_active_at(&tuesday_before_end));

        // Tuesday 5:00:00 UTC = 6:00:00 London - at end time, should be inactive
        let tuesday_at_end = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 7, 1) // Tuesday
                .unwrap()
                .and_hms_opt(5, 0, 0)
                .unwrap(),
            Utc,
        );
        assert!(!schedule.is_active_at(&tuesday_at_end));

        // Friday 21:00:01 UTC = 22:00:01 London - after start time but on Friday which is excluded
        let friday_night = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 7, 4) // Friday
                .unwrap()
                .and_hms_opt(21, 0, 1)
                .unwrap(),
            Utc,
        );
        assert!(!schedule.is_active_at(&friday_night));

        // Thursday 21:00:01 UTC = 22:00:01 London - Thursday night session should be active
        let thursday_night = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 7, 3) // Thursday
                .unwrap()
                .and_hms_opt(21, 0, 1)
                .unwrap(),
            Utc,
        );
        assert!(schedule.is_active_at(&thursday_night));

        // Friday 4:59:59 UTC = 5:59:59 London - still Thursday's session ending on Friday morning
        let friday_morning = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 7, 4) // Friday
                .unwrap()
                .and_hms_opt(4, 59, 59)
                .unwrap(),
            Utc,
        );
        assert!(schedule.is_active_at(&friday_morning));
    }

    #[test]
    fn test_into_non_stop_no_config() {
        let config = ScheduleConfig {
            start_time: None,
            end_time: None,
            start_day: None,
            end_day: None,
            weekdays: vec![],
            timezone: None,
        };

        let schedule = SessionSchedule::try_from(&config).unwrap();
        assert!(matches!(schedule, SessionSchedule::NonStop));
    }

    #[test]
    fn test_into_non_stop_equal_times() {
        let time = NaiveTime::from_hms_opt(9, 0, 0).unwrap();
        let config = ScheduleConfig {
            start_time: Some(time),
            end_time: Some(time),
            start_day: None,
            end_day: None,
            weekdays: vec![],
            timezone: None,
        };

        let schedule = SessionSchedule::try_from(&config).unwrap();
        assert!(matches!(schedule, SessionSchedule::NonStop));
    }

    #[test]
    fn test_into_daily_session() {
        let config = ScheduleConfig {
            start_time: Some(NaiveTime::from_hms_opt(9, 0, 0).unwrap()),
            end_time: Some(NaiveTime::from_hms_opt(17, 0, 0).unwrap()),
            start_day: None,
            end_day: None,
            weekdays: vec![],
            timezone: None,
        };

        let schedule = SessionSchedule::try_from(&config).unwrap();
        match schedule {
            SessionSchedule::Daily {
                start_time,
                end_time,
                timezone,
            } => {
                assert_eq!(start_time, NaiveTime::from_hms_opt(9, 0, 0).unwrap());
                assert_eq!(end_time, NaiveTime::from_hms_opt(17, 0, 0).unwrap());
                assert_eq!(timezone, Tz::UTC);
            }
            _ => panic!("Expected Daily schedule"),
        }
    }

    #[test]
    fn test_into_weekdays_session() {
        let config = ScheduleConfig {
            start_time: Some(NaiveTime::from_hms_opt(9, 0, 0).unwrap()),
            end_time: Some(NaiveTime::from_hms_opt(17, 0, 0).unwrap()),
            start_day: None,
            end_day: None,
            weekdays: vec![
                Weekday::Mon,
                Weekday::Tue,
                Weekday::Wed,
                Weekday::Thu,
                Weekday::Fri,
            ],
            timezone: Some(Tz::Europe__London),
        };

        let schedule = SessionSchedule::try_from(&config).unwrap();
        match schedule {
            SessionSchedule::Weekdays {
                start_time,
                end_time,
                weekdays,
                timezone,
            } => {
                assert_eq!(start_time, NaiveTime::from_hms_opt(9, 0, 0).unwrap());
                assert_eq!(end_time, NaiveTime::from_hms_opt(17, 0, 0).unwrap());
                assert_eq!(
                    weekdays,
                    vec![
                        Weekday::Mon,
                        Weekday::Tue,
                        Weekday::Wed,
                        Weekday::Thu,
                        Weekday::Fri
                    ]
                );
                assert_eq!(timezone, Tz::Europe__London);
            }
            _ => panic!("Expected Weekdays schedule"),
        }
    }

    #[test]
    fn test_into_weekly_session_valid() {
        let config = ScheduleConfig {
            start_time: Some(NaiveTime::from_hms_opt(18, 0, 0).unwrap()),
            end_time: Some(NaiveTime::from_hms_opt(17, 0, 0).unwrap()),
            start_day: Some(Weekday::Sun),
            end_day: Some(Weekday::Fri),
            weekdays: vec![],
            timezone: Some(Tz::America__New_York),
        };

        let schedule = SessionSchedule::try_from(&config).unwrap();
        match schedule {
            SessionSchedule::Weekly {
                start_day,
                start_time,
                end_day,
                end_time,
                timezone,
            } => {
                assert_eq!(start_day, Weekday::Sun);
                assert_eq!(start_time, NaiveTime::from_hms_opt(18, 0, 0).unwrap());
                assert_eq!(end_day, Weekday::Fri);
                assert_eq!(end_time, NaiveTime::from_hms_opt(17, 0, 0).unwrap());
                assert_eq!(timezone, Tz::America__New_York);
            }
            _ => panic!("Expected Weekly schedule"),
        }
    }

    #[test]
    fn test_into_weekly_session_equal_times_distinct_days_valid() {
        let time = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
        let config = ScheduleConfig {
            start_time: Some(time),
            end_time: Some(time),
            start_day: Some(Weekday::Mon),
            end_day: Some(Weekday::Fri),
            weekdays: vec![],
            timezone: None,
        };

        let schedule = SessionSchedule::try_from(&config).unwrap();
        assert!(matches!(schedule, SessionSchedule::Weekly { .. }));
    }

    #[test]
    fn test_into_weekly_session_same_day_and_start_gt_end_valid() {
        let start = NaiveTime::from_hms_opt(12, 0, 0).unwrap();
        let end = NaiveTime::from_hms_opt(9, 0, 0).unwrap();
        let config = ScheduleConfig {
            start_time: Some(start),
            end_time: Some(end),
            start_day: Some(Weekday::Mon),
            end_day: Some(Weekday::Mon),
            weekdays: vec![],
            timezone: None,
        };

        let schedule = SessionSchedule::try_from(&config).unwrap();
        assert!(matches!(schedule, SessionSchedule::Weekly { .. }));
    }

    #[test]
    fn test_into_weekly_session_same_day_and_same_time_collapses_to_nonstop() {
        let config = ScheduleConfig {
            start_time: Some(NaiveTime::from_hms_opt(9, 0, 0).unwrap()),
            end_time: Some(NaiveTime::from_hms_opt(9, 0, 0).unwrap()),
            start_day: Some(Weekday::Mon),
            end_day: Some(Weekday::Mon),
            weekdays: vec![],
            timezone: None,
        };

        let schedule = SessionSchedule::try_from(&config).unwrap();
        assert!(matches!(schedule, SessionSchedule::NonStop));
    }

    #[test]
    fn test_into_weekly_session_same_day_wrong_time_error() {
        let config = ScheduleConfig {
            start_time: Some(NaiveTime::from_hms_opt(1, 0, 0).unwrap()),
            end_time: Some(NaiveTime::from_hms_opt(9, 0, 0).unwrap()),
            start_day: Some(Weekday::Mon),
            end_day: Some(Weekday::Mon),
            weekdays: vec![],
            timezone: None,
        };

        let result = SessionSchedule::try_from(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_into_invalid_weekly_with_weekdays() {
        let config = ScheduleConfig {
            start_time: Some(NaiveTime::from_hms_opt(9, 0, 0).unwrap()),
            end_time: Some(NaiveTime::from_hms_opt(17, 0, 0).unwrap()),
            start_day: Some(Weekday::Mon),
            end_day: Some(Weekday::Fri),
            weekdays: vec![Weekday::Mon],
            timezone: None,
        };

        let result = SessionSchedule::try_from(&config);
        assert!(result.is_err());
        match result.unwrap_err() {
            SessionCreationError::InvalidSchedule(msg) => {
                assert!(msg.contains("weekly sessions cannot have weekdays specified"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[test]
    fn test_into_invalid_partial_config_start_time_only() {
        let config = ScheduleConfig {
            start_time: Some(NaiveTime::from_hms_opt(9, 0, 0).unwrap()),
            end_time: None,
            start_day: None,
            end_day: None,
            weekdays: vec![],
            timezone: None,
        };

        let result = SessionSchedule::try_from(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_into_invalid_partial_config_end_time_only() {
        let config = ScheduleConfig {
            start_time: None,
            end_time: Some(NaiveTime::from_hms_opt(17, 0, 0).unwrap()),
            start_day: None,
            end_day: None,
            weekdays: vec![],
            timezone: None,
        };

        let result = SessionSchedule::try_from(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_into_invalid_partial_config_start_day_only() {
        let config = ScheduleConfig {
            start_time: None,
            end_time: None,
            start_day: Some(Weekday::Mon),
            end_day: None,
            weekdays: vec![],
            timezone: None,
        };

        let result = SessionSchedule::try_from(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_into_invalid_mixed_config() {
        let config = ScheduleConfig {
            start_time: Some(NaiveTime::from_hms_opt(9, 0, 0).unwrap()),
            end_time: None,
            start_day: Some(Weekday::Mon),
            end_day: None,
            weekdays: vec![],
            timezone: None,
        };

        let result = SessionSchedule::try_from(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_into_weekdays_with_single_day() {
        let config = ScheduleConfig {
            start_time: Some(NaiveTime::from_hms_opt(9, 0, 0).unwrap()),
            end_time: Some(NaiveTime::from_hms_opt(17, 0, 0).unwrap()),
            start_day: None,
            end_day: None,
            weekdays: vec![Weekday::Sat],
            timezone: None,
        };

        let schedule = SessionSchedule::try_from(&config).unwrap();
        match schedule {
            SessionSchedule::Weekdays { weekdays, .. } => {
                assert_eq!(weekdays, vec![Weekday::Sat]);
            }
            _ => panic!("Expected Weekdays schedule"),
        }
    }

    #[test]
    fn test_into_weekdays_with_equal_times_is_invalid() {
        let time = NaiveTime::from_hms_opt(10, 30, 0).unwrap();
        let config = ScheduleConfig {
            start_time: Some(time),
            end_time: Some(time),
            start_day: None,
            end_day: None,
            weekdays: vec![Weekday::Mon, Weekday::Wed, Weekday::Fri],
            timezone: None,
        };

        let schedule = SessionSchedule::try_from(&config);
        assert!(schedule.is_err());
    }

    #[test]
    fn test_is_same_session_period_nonstop() {
        let schedule = SessionSchedule::NonStop;

        let dt1 = DateTime::parse_from_rfc3339("2025-01-15T01:30:00-05:00")
            .unwrap()
            .to_utc();
        let dt2 = DateTime::parse_from_rfc3339("2026-01-15T23:30:00-05:00")
            .unwrap()
            .to_utc();
        assert_eq!(
            schedule.is_same_session_period(&dt1, &dt2).unwrap(),
            SessionPeriodComparison::SamePeriod
        );
    }

    #[test]
    fn test_is_same_session_period_daily_utc() {
        let schedule = SessionSchedule::Daily {
            start_time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
            end_time: NaiveTime::from_hms_opt(17, 0, 0).unwrap(),
            timezone: Tz::UTC,
        };

        // two times within the same session period return SamePeriod
        let dt1 = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 6, 27)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            Utc,
        );
        let dt2 = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 6, 27)
                .unwrap()
                .and_hms_opt(15, 0, 0)
                .unwrap(),
            Utc,
        );
        assert_eq!(
            schedule.is_same_session_period(&dt1, &dt2).unwrap(),
            SessionPeriodComparison::SamePeriod
        );
        assert_eq!(
            schedule.is_same_session_period(&dt2, &dt1).unwrap(),
            SessionPeriodComparison::SamePeriod
        );

        // time for the next session period returns DifferentPeriod
        let dt3 = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 6, 28)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            Utc,
        );
        assert_eq!(
            schedule.is_same_session_period(&dt1, &dt3).unwrap(),
            SessionPeriodComparison::DifferentPeriod
        );
        assert_eq!(
            schedule.is_same_session_period(&dt3, &dt1).unwrap(),
            SessionPeriodComparison::DifferentPeriod
        );

        // time on the same day but outside session time returns OutsideSessionTime
        let dt4 = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 6, 27)
                .unwrap()
                .and_hms_opt(19, 0, 0)
                .unwrap(),
            Utc,
        );
        assert!(matches!(
            schedule.is_same_session_period(&dt1, &dt4).unwrap(),
            SessionPeriodComparison::OutsideSessionTime {
                which: WhichTime::Second
            }
        ));
        assert!(matches!(
            schedule.is_same_session_period(&dt4, &dt1).unwrap(),
            SessionPeriodComparison::OutsideSessionTime {
                which: WhichTime::First
            }
        ));
    }

    #[test]
    fn test_is_same_session_period_daily_nyc() {
        let schedule = SessionSchedule::Daily {
            start_time: NaiveTime::from_hms_opt(1, 0, 0).unwrap(),
            end_time: NaiveTime::from_hms_opt(23, 0, 0).unwrap(),
            timezone: Tz::America__New_York,
        };

        // same session period on the same day (both in EST)
        let dt1 = DateTime::parse_from_rfc3339("2025-01-15T01:30:00-05:00")
            .unwrap()
            .to_utc();
        let dt2 = DateTime::parse_from_rfc3339("2025-01-15T22:45:00-05:00")
            .unwrap()
            .to_utc();
        assert_eq!(
            schedule.is_same_session_period(&dt1, &dt2).unwrap(),
            SessionPeriodComparison::SamePeriod
        );

        // different session periods on consecutive days
        let dt3 = DateTime::parse_from_rfc3339("2024-01-15T22:30:00-05:00")
            .unwrap()
            .to_utc();
        let dt4 = DateTime::parse_from_rfc3339("2024-01-16T02:30:00-05:00")
            .unwrap()
            .to_utc();
        assert_eq!(
            schedule.is_same_session_period(&dt3, &dt4).unwrap(),
            SessionPeriodComparison::DifferentPeriod
        );

        // session boundary testing - end of session vs start of next session
        let dt5 = DateTime::parse_from_rfc3339("2024-01-15T22:59:59-05:00")
            .unwrap()
            .to_utc();
        let dt6 = DateTime::parse_from_rfc3339("2024-01-16T01:00:01-05:00")
            .unwrap()
            .to_utc();
        assert_eq!(
            schedule.is_same_session_period(&dt5, &dt6).unwrap(),
            SessionPeriodComparison::DifferentPeriod
        );

        // time that doesn't fall into any session period
        let dt7 = DateTime::parse_from_rfc3339("2024-01-15T23:30:00-05:00")
            .unwrap()
            .to_utc();
        let dt8 = DateTime::parse_from_rfc3339("2024-01-15T10:00:00-05:00")
            .unwrap()
            .to_utc();
        assert!(matches!(
            schedule.is_same_session_period(&dt7, &dt8).unwrap(),
            SessionPeriodComparison::OutsideSessionTime {
                which: WhichTime::First
            }
        ));
    }

    #[test]
    fn test_is_same_session_period_daily_nyc_with_midnight_crossover() {
        // schedule end time is past midnight
        let schedule = SessionSchedule::Daily {
            start_time: NaiveTime::from_hms_opt(6, 0, 0).unwrap(),
            end_time: NaiveTime::from_hms_opt(1, 0, 0).unwrap(),
            timezone: Tz::America__New_York,
        };

        // same session period on consecutive days
        let dt1 = DateTime::parse_from_rfc3339("2025-01-15T15:30:00-05:00")
            .unwrap()
            .to_utc();
        let dt2 = DateTime::parse_from_rfc3339("2025-01-16T00:45:00-05:00")
            .unwrap()
            .to_utc();
        assert_eq!(
            schedule.is_same_session_period(&dt1, &dt2).unwrap(),
            SessionPeriodComparison::SamePeriod
        );
        assert_eq!(
            schedule.is_same_session_period(&dt2, &dt1).unwrap(),
            SessionPeriodComparison::SamePeriod
        );

        // different session period on the same day
        let dt1 = DateTime::parse_from_rfc3339("2025-01-15T15:30:00-05:00")
            .unwrap()
            .to_utc();
        let dt2 = DateTime::parse_from_rfc3339("2025-01-15T00:45:00-05:00")
            .unwrap()
            .to_utc();
        assert_eq!(
            schedule.is_same_session_period(&dt1, &dt2).unwrap(),
            SessionPeriodComparison::DifferentPeriod
        );
        assert_eq!(
            schedule.is_same_session_period(&dt2, &dt1).unwrap(),
            SessionPeriodComparison::DifferentPeriod
        );
    }

    #[test]
    fn test_is_same_session_period_weekdays_utc() {
        let schedule = SessionSchedule::Weekdays {
            start_time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
            end_time: NaiveTime::from_hms_opt(17, 0, 0).unwrap(),
            weekdays: vec![
                Weekday::Mon,
                Weekday::Tue,
                Weekday::Wed,
                Weekday::Thu,
                Weekday::Fri,
            ],
            timezone: Tz::UTC,
        };

        // 10/07/2025 is a Thursday
        // two times within the same session period return SamePeriod
        let dt1 = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 7, 10)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            Utc,
        );
        let dt2 = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 7, 10)
                .unwrap()
                .and_hms_opt(15, 0, 0)
                .unwrap(),
            Utc,
        );
        assert_eq!(
            schedule.is_same_session_period(&dt1, &dt2).unwrap(),
            SessionPeriodComparison::SamePeriod
        );
        assert_eq!(
            schedule.is_same_session_period(&dt2, &dt1).unwrap(),
            SessionPeriodComparison::SamePeriod
        );

        // time for the next session period returns DifferentPeriod
        let dt3 = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 7, 11)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            Utc,
        );
        assert_eq!(
            schedule.is_same_session_period(&dt1, &dt3).unwrap(),
            SessionPeriodComparison::DifferentPeriod
        );
        assert_eq!(
            schedule.is_same_session_period(&dt3, &dt1).unwrap(),
            SessionPeriodComparison::DifferentPeriod
        );

        // time on the same day but outside session time returns OutsideSessionTime
        let dt4 = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 7, 10)
                .unwrap()
                .and_hms_opt(19, 0, 0)
                .unwrap(),
            Utc,
        );
        assert!(matches!(
            schedule.is_same_session_period(&dt1, &dt4).unwrap(),
            SessionPeriodComparison::OutsideSessionTime {
                which: WhichTime::Second
            }
        ));
        assert!(matches!(
            schedule.is_same_session_period(&dt4, &dt1).unwrap(),
            SessionPeriodComparison::OutsideSessionTime {
                which: WhichTime::First
            }
        ));

        // time falls on the Saturday (outside session period) returns OutsideSessionTime
        let dt5 = DateTime::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2025, 7, 12)
                .unwrap()
                .and_hms_opt(13, 0, 0)
                .unwrap(),
            Utc,
        );
        assert!(matches!(
            schedule.is_same_session_period(&dt1, &dt5).unwrap(),
            SessionPeriodComparison::OutsideSessionTime {
                which: WhichTime::Second
            }
        ));
        assert!(matches!(
            schedule.is_same_session_period(&dt5, &dt1).unwrap(),
            SessionPeriodComparison::OutsideSessionTime {
                which: WhichTime::First
            }
        ));
    }

    #[test]
    fn test_active_at_weekly_schedule_utc() {
        // Mon 09:00 → Wed 17:00 UTC
        let schedule = SessionSchedule::Weekly {
            start_day: Weekday::Mon,
            start_time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
            end_day: Weekday::Wed,
            end_time: NaiveTime::from_hms_opt(17, 0, 0).unwrap(),
            timezone: Tz::UTC,
        };

        // Sunday before — inactive
        assert!(!schedule.is_active_at(&utc_dt(2025, 6, 29, 12, 0, 0)));
        // Monday 8:59:59 — inactive (before start)
        assert!(!schedule.is_active_at(&utc_dt(2025, 6, 30, 8, 59, 59)));
        // Monday 9:00:00 — active (inclusive start)
        assert!(schedule.is_active_at(&utc_dt(2025, 6, 30, 9, 0, 0)));
        // Tuesday 03:00 — active (mid-session, full day)
        assert!(schedule.is_active_at(&utc_dt(2025, 7, 1, 3, 0, 0)));
        // Wednesday 16:59:59 — active
        assert!(schedule.is_active_at(&utc_dt(2025, 7, 2, 16, 59, 59)));
        // Wednesday 17:00:00 — inactive (exclusive end)
        assert!(!schedule.is_active_at(&utc_dt(2025, 7, 2, 17, 0, 0)));
        // Thursday — inactive
        assert!(!schedule.is_active_at(&utc_dt(2025, 7, 3, 10, 0, 0)));
        // Saturday — inactive
        assert!(!schedule.is_active_at(&utc_dt(2025, 7, 5, 12, 0, 0)));
    }

    #[test]
    fn test_active_at_weekly_schedule_24x5_new_york() {
        // Classic 24/5 FIX trading week: Sun 18:00 → Fri 17:00 New York time.
        // In January, New York is EST (UTC-5).
        let schedule = SessionSchedule::Weekly {
            start_day: Weekday::Sun,
            start_time: NaiveTime::from_hms_opt(18, 0, 0).unwrap(),
            end_day: Weekday::Fri,
            end_time: NaiveTime::from_hms_opt(17, 0, 0).unwrap(),
            timezone: Tz::America__New_York,
        };

        // Sun Jan 12, 2025 22:59:59 UTC = 17:59:59 NY — just before start
        assert!(!schedule.is_active_at(&utc_dt(2025, 1, 12, 22, 59, 59)));
        // Sun Jan 12, 2025 23:00:01 UTC = 18:00:01 NY — just after start
        assert!(schedule.is_active_at(&utc_dt(2025, 1, 12, 23, 0, 1)));
        // Wed Jan 15, 2025 12:00 UTC = 07:00 NY — mid session
        assert!(schedule.is_active_at(&utc_dt(2025, 1, 15, 12, 0, 0)));
        // Fri Jan 17, 2025 21:59:59 UTC = 16:59:59 NY — just before end
        assert!(schedule.is_active_at(&utc_dt(2025, 1, 17, 21, 59, 59)));
        // Fri Jan 17, 2025 22:00:00 UTC = 17:00:00 NY — at end (exclusive)
        assert!(!schedule.is_active_at(&utc_dt(2025, 1, 17, 22, 0, 0)));
        // Sat Jan 18, 2025 12:00 UTC = 07:00 NY Sat — outside session window
        assert!(!schedule.is_active_at(&utc_dt(2025, 1, 18, 12, 0, 0)));
        // Sun Jan 19, 2025 12:00 UTC = 07:00 NY Sun — before next session
        assert!(!schedule.is_active_at(&utc_dt(2025, 1, 19, 12, 0, 0)));
    }

    #[test]
    fn test_active_at_weekly_schedule_london() {
        // Mon 09:00 → Fri 17:00 London time. June 2025: London is UTC+1 (BST).
        let schedule = SessionSchedule::Weekly {
            start_day: Weekday::Mon,
            start_time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
            end_day: Weekday::Fri,
            end_time: NaiveTime::from_hms_opt(17, 0, 0).unwrap(),
            timezone: Tz::Europe__London,
        };

        // Mon Jun 30, 2025 07:59:59 UTC = 08:59:59 London — before start
        assert!(!schedule.is_active_at(&utc_dt(2025, 6, 30, 7, 59, 59)));
        // Mon Jun 30, 2025 08:00:01 UTC = 09:00:01 London — after start
        assert!(schedule.is_active_at(&utc_dt(2025, 6, 30, 8, 0, 1)));
        // Fri Jul 4, 2025 15:59:59 UTC = 16:59:59 London — just before end
        assert!(schedule.is_active_at(&utc_dt(2025, 7, 4, 15, 59, 59)));
        // Fri Jul 4, 2025 16:00:00 UTC = 17:00:00 London — at end (exclusive)
        assert!(!schedule.is_active_at(&utc_dt(2025, 7, 4, 16, 0, 0)));
    }

    #[test]
    fn test_active_at_weekly_schedule_short_window() {
        // Short cross-day window: Tue 22:00 → Wed 06:00 UTC (8h, crosses midnight)
        let schedule = SessionSchedule::Weekly {
            start_day: Weekday::Tue,
            start_time: NaiveTime::from_hms_opt(22, 0, 0).unwrap(),
            end_day: Weekday::Wed,
            end_time: NaiveTime::from_hms_opt(6, 0, 0).unwrap(),
            timezone: Tz::UTC,
        };

        // Tue 21:59:59 — inactive
        assert!(!schedule.is_active_at(&utc_dt(2025, 7, 1, 21, 59, 59)));
        // Tue 22:00:00 — active
        assert!(schedule.is_active_at(&utc_dt(2025, 7, 1, 22, 0, 0)));
        // Tue 23:30 — active
        assert!(schedule.is_active_at(&utc_dt(2025, 7, 1, 23, 30, 0)));
        // Wed 02:00 — active (across midnight)
        assert!(schedule.is_active_at(&utc_dt(2025, 7, 2, 2, 0, 0)));
        // Wed 05:59:59 — active
        assert!(schedule.is_active_at(&utc_dt(2025, 7, 2, 5, 59, 59)));
        // Wed 06:00:00 — inactive (exclusive end)
        assert!(!schedule.is_active_at(&utc_dt(2025, 7, 2, 6, 0, 0)));
        // Wed 12:00 — inactive
        assert!(!schedule.is_active_at(&utc_dt(2025, 7, 2, 12, 0, 0)));
    }

    #[test]
    fn test_active_at_weekly_schedule_full_week_minus_gap() {
        // Very wide window: Mon 02:00 → Sun 23:00 UTC
        let schedule = SessionSchedule::Weekly {
            start_day: Weekday::Mon,
            start_time: NaiveTime::from_hms_opt(2, 0, 0).unwrap(),
            end_day: Weekday::Sun,
            end_time: NaiveTime::from_hms_opt(23, 0, 0).unwrap(),
            timezone: Tz::UTC,
        };

        // Mon 01:59:59 — inactive (just before start)
        assert!(!schedule.is_active_at(&utc_dt(2025, 6, 30, 1, 59, 59)));
        // Mon 02:00:00 — active
        assert!(schedule.is_active_at(&utc_dt(2025, 6, 30, 2, 0, 0)));
        // Mid-week — active
        assert!(schedule.is_active_at(&utc_dt(2025, 7, 2, 12, 0, 0)));
        // Sat — active
        assert!(schedule.is_active_at(&utc_dt(2025, 7, 5, 18, 0, 0)));
        // Sun 22:59:59 — active
        assert!(schedule.is_active_at(&utc_dt(2025, 7, 6, 22, 59, 59)));
        // Sun 23:00:00 — inactive (exclusive end)
        assert!(!schedule.is_active_at(&utc_dt(2025, 7, 6, 23, 0, 0)));
    }

    #[test]
    fn test_is_same_session_period_weekly_utc() {
        // Mon 09:00 → Fri 17:00 UTC
        let schedule = SessionSchedule::Weekly {
            start_day: Weekday::Mon,
            start_time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
            end_day: Weekday::Fri,
            end_time: NaiveTime::from_hms_opt(17, 0, 0).unwrap(),
            timezone: Tz::UTC,
        };

        let dt_mon = utc_dt(2025, 6, 30, 10, 0, 0);
        let dt_thu = utc_dt(2025, 7, 3, 15, 0, 0);
        assert_eq!(
            schedule.is_same_session_period(&dt_mon, &dt_thu).unwrap(),
            SessionPeriodComparison::SamePeriod
        );
        assert_eq!(
            schedule.is_same_session_period(&dt_thu, &dt_mon).unwrap(),
            SessionPeriodComparison::SamePeriod
        );

        // Same time-of-week but next week's session → DifferentPeriod
        let dt_mon_next = utc_dt(2025, 7, 7, 10, 0, 0);
        assert_eq!(
            schedule
                .is_same_session_period(&dt_mon, &dt_mon_next)
                .unwrap(),
            SessionPeriodComparison::DifferentPeriod
        );

        // Saturday during the gap → OutsideSessionTime
        let dt_sat = utc_dt(2025, 7, 5, 12, 0, 0);
        assert!(matches!(
            schedule.is_same_session_period(&dt_mon, &dt_sat).unwrap(),
            SessionPeriodComparison::OutsideSessionTime {
                which: WhichTime::Second
            }
        ));
        assert!(matches!(
            schedule.is_same_session_period(&dt_sat, &dt_mon).unwrap(),
            SessionPeriodComparison::OutsideSessionTime {
                which: WhichTime::First
            }
        ));
    }

    #[test]
    fn test_is_same_session_period_weekly_at_boundary() {
        // Mon 09:00 → Fri 17:00 UTC — verify last-second/first-second of consecutive sessions
        // are classified as DifferentPeriod.
        let schedule = SessionSchedule::Weekly {
            start_day: Weekday::Mon,
            start_time: NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
            end_day: Weekday::Fri,
            end_time: NaiveTime::from_hms_opt(17, 0, 0).unwrap(),
            timezone: Tz::UTC,
        };

        // Last active second of week-of-Jun-30 vs first active second of next week.
        let last_sec = utc_dt(2025, 7, 4, 16, 59, 59);
        let next_first = utc_dt(2025, 7, 7, 9, 0, 0);
        assert_eq!(
            schedule
                .is_same_session_period(&last_sec, &next_first)
                .unwrap(),
            SessionPeriodComparison::DifferentPeriod
        );
    }

    #[test]
    fn test_is_same_session_period_weekly_24x5_newyork_across_weeks() {
        // Sun 18:00 → Fri 17:00 New York (wrap case). Verify two times that wrap across the
        // calendar week into the same session window are classified as SamePeriod.
        let schedule = SessionSchedule::Weekly {
            start_day: Weekday::Sun,
            start_time: NaiveTime::from_hms_opt(18, 0, 0).unwrap(),
            end_day: Weekday::Fri,
            end_time: NaiveTime::from_hms_opt(17, 0, 0).unwrap(),
            timezone: Tz::America__New_York,
        };

        // Sun Jan 12 23:30 UTC = Sun 18:30 NY — start of session
        let dt_sun = utc_dt(2025, 1, 12, 23, 30, 0);
        // Wed Jan 15 14:00 UTC = Wed 09:00 NY — mid session
        let dt_wed = utc_dt(2025, 1, 15, 14, 0, 0);
        // Fri Jan 17 21:00 UTC = Fri 16:00 NY — near end of same session
        let dt_fri = utc_dt(2025, 1, 17, 21, 0, 0);

        assert_eq!(
            schedule.is_same_session_period(&dt_sun, &dt_wed).unwrap(),
            SessionPeriodComparison::SamePeriod
        );
        assert_eq!(
            schedule.is_same_session_period(&dt_wed, &dt_fri).unwrap(),
            SessionPeriodComparison::SamePeriod
        );
        assert_eq!(
            schedule.is_same_session_period(&dt_sun, &dt_fri).unwrap(),
            SessionPeriodComparison::SamePeriod
        );

        // Following week's Monday (= same session continues from Sun Jan 19 18:00 NY)
        // Sun Jan 19 23:30 UTC = Sun 18:30 NY — start of NEXT session
        let dt_sun_next = utc_dt(2025, 1, 19, 23, 30, 0);
        assert_eq!(
            schedule
                .is_same_session_period(&dt_sun, &dt_sun_next)
                .unwrap(),
            SessionPeriodComparison::DifferentPeriod
        );
    }

    #[test]
    fn test_is_same_session_period_weekly_within_wrap() {
        // Sun 18:00 → Fri 17:00 UTC (wrap). Use UTC to avoid TZ confusion in the assertions.
        let schedule = SessionSchedule::Weekly {
            start_day: Weekday::Sun,
            start_time: NaiveTime::from_hms_opt(18, 0, 0).unwrap(),
            end_day: Weekday::Fri,
            end_time: NaiveTime::from_hms_opt(17, 0, 0).unwrap(),
            timezone: Tz::UTC,
        };

        // Two times that span the calendar week boundary (Sun → Mon) but within the same
        // weekly session — must be SamePeriod.
        let sun_eve = utc_dt(2025, 6, 29, 19, 0, 0); // Sunday after start
        let mon_morning = utc_dt(2025, 6, 30, 8, 0, 0); // Monday early — same session
        assert_eq!(
            schedule
                .is_same_session_period(&sun_eve, &mon_morning)
                .unwrap(),
            SessionPeriodComparison::SamePeriod
        );

        // Saturday in the gap → OutsideSessionTime
        let sat = utc_dt(2025, 7, 5, 12, 0, 0);
        assert!(matches!(
            schedule.is_same_session_period(&sun_eve, &sat).unwrap(),
            SessionPeriodComparison::OutsideSessionTime {
                which: WhichTime::Second
            }
        ));
    }

    #[test]
    fn construct_utc_at_gap() {
        // Test DST gap (spring forward) - 2:30 AM doesn't exist on March 10, 2024 in US/Eastern
        let date = NaiveDate::from_ymd_opt(2024, 3, 10).unwrap();
        let time = NaiveTime::from_hms_opt(2, 30, 0).unwrap(); // This time is skipped during DST transition
        let timezone = chrono_tz::US::Eastern;

        let result = construct_utc(&date, &time, &timezone);

        assert!(matches!(
            result,
            Err(ScheduleError::AmbiguousOrMissingTime { .. })
        ));
    }

    #[test]
    fn construct_utc_at_fold() {
        // Test DST fold (fall back) - 1:30 AM occurs twice on November 3, 2024 in US/Eastern
        let date = NaiveDate::from_ymd_opt(2024, 11, 3).unwrap();
        let time = NaiveTime::from_hms_opt(1, 30, 0).unwrap(); // This time exists twice during DST transition
        let timezone = chrono_tz::US::Eastern;

        let result = construct_utc(&date, &time, &timezone);

        assert!(matches!(
            result,
            Err(ScheduleError::AmbiguousOrMissingTime { .. })
        ));
    }
}
