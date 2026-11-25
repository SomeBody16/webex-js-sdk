/**
 * SPARK-871464 diagnostics (temporary, not for release).
 *
 * Every log line is prefixed with `[SPARK-871464]` so the guest-leave / cleanup race can be grepped
 * out of uploaded client logs.
 *
 * Logging only: no behavior changes.
 */
import LoggerProxy from '../common/logs/logger-proxy';

export const SPARK_871464_TAG = '[SPARK-871464]';

const WATCHDOG_INTERVAL_MS = 5000;

type CleanupRecord = {
  id: number;
  source: string;
  startedAt: number;
  step: string;
};

let cleanupCounter = 0;
const nextCleanupSource = new WeakMap<object, string>();
const cleanupsByMeeting = new WeakMap<object, Set<CleanupRecord>>();

export const spark871464Log = (message: string) => {
  LoggerProxy.logger.info(`${SPARK_871464_TAG} ${message}`);
};

export const spark871464Warn = (message: string) => {
  LoggerProxy.logger.warn(`${SPARK_871464_TAG} ${message}`);
};

const startWatchdog = (onTick: () => void) => {
  const watchdog: any = setInterval(onTick, WATCHDOG_INTERVAL_MS);

  // don't keep Node (unit tests) alive because of a diagnostic timer
  watchdog?.unref?.();

  return () => clearInterval(watchdog);
};

export const describeMeeting = (meeting: any) =>
  `meetingId=${meeting?.id}; correlationId=${meeting?.correlationId}`;

const describeCleanups = (meeting: any) =>
  Array.from(cleanupsByMeeting.get(meeting) ?? [])
    .map((c) => `#${c.id}:${c.source}@${c.step}/${Date.now() - c.startedAt}ms`)
    .join(', ');

/**
 * Labels the next MeetingUtil.cleanUp() call for this meeting (cleanUp's signature is unchanged).
 * @param {Object} meeting
 * @param {string} source
 * @returns {void}
 */
export const setNextCleanupSource = (meeting: any, source: string) => {
  if (meeting && typeof meeting === 'object') {
    nextCleanupSource.set(meeting, source);
  }
};

export const startCleanupTracking = (meeting: any, preserveVoiceaChannel: boolean) => {
  const key = meeting && typeof meeting === 'object' ? meeting : {};
  const source = nextCleanupSource.get(key) ?? 'unknown';

  nextCleanupSource.delete(key);
  cleanupCounter += 1;

  const record: CleanupRecord = {id: cleanupCounter, source, startedAt: Date.now(), step: 'start'};
  const label = () =>
    `cleanupId=${record.id}; source=${record.source}; ${describeMeeting(meeting)}`;
  const running = describeCleanups(key);

  spark871464Log(
    `Meeting:util#cleanUp --> Starting cleanup; ${label()}; preserveVoiceaChannel=${preserveVoiceaChannel}; concurrentCleanups=[${running}]`
  );
  if (running) {
    spark871464Warn(
      `Meeting:util#cleanUp --> OVERLAPPING CLEANUP: cleanupId=${record.id} (${source}) started while [${running}] is still running`
    );
  }

  let set = cleanupsByMeeting.get(key);

  if (!set) {
    set = new Set();
    cleanupsByMeeting.set(key, set);
  }
  set.add(record);

  const stopWatchdog = startWatchdog(() => {
    spark871464Warn(
      `Meeting:util#cleanUp --> STILL PENDING after ${Date.now() - record.startedAt}ms at step="${
        record.step
      }"; ${label()}; concurrentCleanups=[${describeCleanups(key)}]`
    );
  });

  const finish = () => {
    stopWatchdog();
    set.delete(record);
  };

  return {
    step(step: string) {
      record.step = step;
      spark871464Log(
        `Meeting:util#cleanUp --> ${step} after ${Date.now() - record.startedAt}ms; ${label()}`
      );
    },
    done() {
      finish();
      spark871464Log(
        `Meeting:util#cleanUp --> Completed after ${Date.now() - record.startedAt}ms; ${label()}`
      );
    },
    fail(error: any) {
      finish();
      spark871464Warn(
        `Meeting:util#cleanUp --> Failed after ${Date.now() - record.startedAt}ms at step="${
          record.step
        }"; ${label()}; error=${error}`
      );
    },
  };
};

/**
 * Logs start/settle of a promise and warns every few seconds while it is still pending.
 * @param {string} label
 * @param {Object} meeting
 * @param {Promise} promise
 * @returns {Promise} the same outcome as `promise`
 */
export const trackPending = <T>(label: string, meeting: any, promise: Promise<T>): Promise<T> => {
  const startedAt = Date.now();
  const stopWatchdog = startWatchdog(() => {
    spark871464Warn(
      `${label} --> STILL PENDING after ${Date.now() - startedAt}ms; ${describeMeeting(
        meeting
      )}; runningCleanups=[${describeCleanups(meeting)}]`
    );
  });

  return promise.then(
    (result) => {
      stopWatchdog();
      spark871464Log(
        `${label} --> resolved after ${Date.now() - startedAt}ms; ${describeMeeting(meeting)}`
      );

      return result;
    },
    (error) => {
      stopWatchdog();
      spark871464Warn(
        `${label} --> rejected after ${Date.now() - startedAt}ms; ${describeMeeting(
          meeting
        )}; error=${error}`
      );

      throw error;
    }
  );
};
