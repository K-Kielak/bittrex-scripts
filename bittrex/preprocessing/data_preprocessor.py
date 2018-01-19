from datetime import timedelta
from dateutil.parser import parse

MAGNITUDE_CHANGE = 10**8

OPEN_LABEL = 'O'
HIGH_LABEL = 'H'
LOW_LABEL = 'L'
CLOSE_LABEL = 'C'
VOLUME_LABEL = 'V'
TIMESPAN_LABEL = 'T'
BASE_VOLUME_LABEL = 'BV'


def fill_empty_timespans(ticks, interval):
    """
    :param ticks: list of market ticks
    :param interval: interval in minutes, if data is for 1 min interval it should be 1, if it's for daily interval
            it should be 1440
    :return: ticks with all the gaps filled with ticks that have all prices set to the same value as the last tick
            closing price and with volume set to 0
    """
    # convert string date to date object to make finding gaps easier
    for t in ticks:
        t[TIMESPAN_LABEL] = parse(t[TIMESPAN_LABEL])

    i = 0
    while i < (len(ticks) - 1):
        time_difference = ticks[i + 1][TIMESPAN_LABEL] - ticks[i][TIMESPAN_LABEL]
        if time_difference / timedelta(minutes=1) != interval:
            filler_tick = {
                OPEN_LABEL: ticks[i][CLOSE_LABEL],
                HIGH_LABEL: ticks[i][CLOSE_LABEL],
                LOW_LABEL: ticks[i][CLOSE_LABEL],
                CLOSE_LABEL: ticks[i][CLOSE_LABEL],
                VOLUME_LABEL: 0,
                TIMESPAN_LABEL: ticks[i][TIMESPAN_LABEL] + timedelta(minutes=interval),
                BASE_VOLUME_LABEL: 0
            }

            ticks.insert(i + 1, filler_tick)

        i += 1

    return ticks


def convert_ticks_to_states(ticks, state_size, ema_size):
    """
    Convert whole ticks to the list of states
    :param ticks: market ticks that will be used to generate states
    :param state_size: expected state size, i.e. how many ticks should one state take into consideration
    :param ema_size: how many ticks before the state should be used to calculate exponential moving average to which
            state ticks will relate
    :return: list of states, each state consisting of state_size state ticks that were related to its EMA
    """
    # change magnitude to avoid vanishing values due to low altcoin prices
    ticks = _change_ticks_magnitude(ticks, MAGNITUDE_CHANGE)
    ticks_needed_for_one_state = state_size + ema_size
    states = []
    for i in range(0, len(ticks) - ticks_needed_for_one_state):
        print('.')  # TODO delete
        # take ticks to calculate ema, reverse them (step -1) so they are sorted in descending importance
        ema_ticks = ticks[(i+ema_size):i:-1]
        ema = calculate_ema(ema_ticks)  # TODO can you calculate ema iteratively so it runs faster?
        state_ticks = convert_ticks_to_state_ticks(ticks[i+ema_size:i+ticks_needed_for_one_state], ema)
        states.append(state_ticks)

    return states


def convert_ticks_to_state_ticks(ticks, ema):
    """
    Converts ticks to state ticks, i.e. relates them to EMA and drops not needed labels
    :param ticks: ticks to convert
    :param ema: exponential moving average to which the ticks are related
    :return: state ticks
    """
    state_ticks = []
    for t in ticks:
        state_ticks.append({
            OPEN_LABEL: 100 * (t[OPEN_LABEL] - ema) / ema,
            HIGH_LABEL: 100 * (t[HIGH_LABEL] - ema) / ema,
            LOW_LABEL: 100 * (t[LOW_LABEL] - ema) / ema,
            CLOSE_LABEL: 100 * (t[CLOSE_LABEL] - ema) / ema,
            VOLUME_LABEL: t[VOLUME_LABEL]
        })

    return state_ticks


def calculate_ema(ticks):
    """
    Calculates exponential moving average tick for given list of ticks
    :param ticks: Ticks for which the EMA is calculated, the higher the index, the less important the tick is
    :return: Exponential moving average based on closing prices of given ticks
    """
    ema = 0
    ticks_left = len(ticks)
    discount = 1
    for t in ticks:
        multiplier = (2.0 / (ticks_left + 1))
        ema += t[CLOSE_LABEL] * discount * multiplier
        discount *= (1 - multiplier)
        ticks_left -= 1

    return ema


def _change_ticks_magnitude(ticks, magnitude_change):
    ticks = ticks[:]
    for t in ticks:
        t[OPEN_LABEL] = t[OPEN_LABEL] * magnitude_change
        t[HIGH_LABEL] = t[HIGH_LABEL] * magnitude_change
        t[LOW_LABEL] = t[LOW_LABEL] * magnitude_change
        t[CLOSE_LABEL] = t[CLOSE_LABEL] * magnitude_change

    return ticks
