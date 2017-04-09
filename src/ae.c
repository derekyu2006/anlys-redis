/* A simple event-driven programming library. Originally I wrote this code
 * for the Jim's event-loop (Jim is a Tcl interpreter) but later translated
 * it in form of a library for easy reuse.
 *
 * Copyright (c) 2006-2010, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *   notice, this list of conditions and the following disclaimer in the
 *   documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *   to endorse or promote products derived from this software without
 *   specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdio.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <poll.h>
#include <string.h>
#include <time.h>
#include <errno.h>

#include "ae.h"
#include "zmalloc.h"
#include "config.h"

/* Include the best multiplexing layer supported by this system.
 * The following should be ordered by performances, descending. */
#ifdef HAVE_EVPORT
#include "ae_evport.c"
#else
  #ifdef HAVE_EPOLL
  #include "ae_epoll.c"
  #else
    #ifdef HAVE_KQUEUE
    #include "ae_kqueue.c"
    #else
    #include "ae_select.c"
    #endif
  #endif
#endif

aeEventLoop *aeCreateEventLoop(int setsize) {
  aeEventLoop *eventLoop;
  int i;

  if ((eventLoop = zmalloc(sizeof(*eventLoop))) == NULL) goto err;
  // 分配io事件event内存空间.
  eventLoop->events = zmalloc(sizeof(aeFileEvent)*setsize);
  // 分配触发事件event内存空间.
  eventLoop->fired = zmalloc(sizeof(aeFiredEvent)*setsize);
  if (eventLoop->events == NULL || eventLoop->fired == NULL) goto err;
  eventLoop->setsize = setsize;
  eventLoop->lastTime = time(NULL);
  // 初始化时间时间链表为空, 也就是没有时间事件.
  eventLoop->timeEventHead = NULL;
  eventLoop->timeEventNextId = 0;
  eventLoop->stop = 0;
  eventLoop->maxfd = -1;
  eventLoop->beforesleep = NULL;
  // 根据系统的实际支持情况调用具体的io复用技术创建循环结构.
  if (aeApiCreate(eventLoop) == -1) goto err;
  // 初始化事件类型掩码为无事件状态
  for (i = 0; i < setsize; i++)
    eventLoop->events[i].mask = AE_NONE;
  return eventLoop;

err:
  if (eventLoop) {
    zfree(eventLoop->events);
    zfree(eventLoop->fired);
    zfree(eventLoop);
  }
  return NULL;
}

/* Return the current set size. */
int aeGetSetSize(aeEventLoop *eventLoop) {
  return eventLoop->setsize;
}

/* Resize the maximum set size of the event loop.
 * If the requested set size is smaller than the current set size, but
 * there is already a file descriptor in use that is >= the requested
 * set size minus one, AE_ERR is returned and the operation is not
 * performed at all.
 *
 * Otherwise AE_OK is returned and the operation is successful. */
int aeResizeSetSize(aeEventLoop *eventLoop, int setsize) {
  int i;

  if (setsize == eventLoop->setsize) return AE_OK;
  if (eventLoop->maxfd >= setsize) return AE_ERR;
  if (aeApiResize(eventLoop,setsize) == -1) return AE_ERR;

  eventLoop->events = zrealloc(eventLoop->events,sizeof(aeFileEvent)*setsize);
  eventLoop->fired = zrealloc(eventLoop->fired,sizeof(aeFiredEvent)*setsize);
  eventLoop->setsize = setsize;

  /* Make sure that if we created new slots, they are initialized with
   * an AE_NONE mask. */
  for (i = eventLoop->maxfd+1; i < setsize; i++)
    eventLoop->events[i].mask = AE_NONE;
  return AE_OK;
}

void aeDeleteEventLoop(aeEventLoop *eventLoop) {
  aeApiFree(eventLoop);
  zfree(eventLoop->events);
  zfree(eventLoop->fired);
  zfree(eventLoop);
}

void aeStop(aeEventLoop *eventLoop) {
  eventLoop->stop = 1;
}

int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask,
    aeFileProc *proc, void *clientData) {
  if (fd >= eventLoop->setsize) {
    errno = ERANGE;
    return AE_ERR;
  }
  aeFileEvent *fe = &eventLoop->events[fd];

  if (aeApiAddEvent(eventLoop, fd, mask) == -1)
    return AE_ERR;
  fe->mask |= mask;
  if (mask & AE_READABLE) fe->rfileProc = proc;
  if (mask & AE_WRITABLE) fe->wfileProc = proc;
  fe->clientData = clientData;
  if (fd > eventLoop->maxfd)
    eventLoop->maxfd = fd;
  return AE_OK;
}

void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask)
{
  if (fd >= eventLoop->setsize) return;
  aeFileEvent *fe = &eventLoop->events[fd];
  if (fe->mask == AE_NONE) return;

  aeApiDelEvent(eventLoop, fd, mask);
  fe->mask = fe->mask & (~mask);
  if (fd == eventLoop->maxfd && fe->mask == AE_NONE) {
    /* Update the max fd */
    int j;

    for (j = eventLoop->maxfd-1; j >= 0; j--)
      if (eventLoop->events[j].mask != AE_NONE) break;
    eventLoop->maxfd = j;
  }
}

int aeGetFileEvents(aeEventLoop *eventLoop, int fd) {
  if (fd >= eventLoop->setsize) return 0;
  aeFileEvent *fe = &eventLoop->events[fd];

  return fe->mask;
}

static void aeGetTime(long *seconds, long *milliseconds)
{
  struct timeval tv;

  gettimeofday(&tv, NULL);
  *seconds = tv.tv_sec;
  *milliseconds = tv.tv_usec/1000;
}

static void aeAddMillisecondsToNow(long long milliseconds, long *sec, long *ms) {
  long cur_sec, cur_ms, when_sec, when_ms;

  aeGetTime(&cur_sec, &cur_ms);
  when_sec = cur_sec + milliseconds/1000;
  when_ms = cur_ms + milliseconds%1000;
  if (when_ms >= 1000) {
    when_sec ++;
    when_ms -= 1000;
  }
  *sec = when_sec;
  *ms = when_ms;
}

long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds,
    aeTimeProc *proc, void *clientData,
    aeEventFinalizerProc *finalizerProc)
{
  /**
   * timeEventNextId会在处理执行定时事件时会用到，用于防止出现死循环.
   * 如果超过了最大 id，则跳过这个定时事件, 为的是避免死循环, 即:
   * 如果事件一执行的时候注册了事件二, 事件一执行完毕后事件二得到执行,
   * 紧接着如果事件一有得到执行就会成为循环, 因此维护了timeEventNextId
   */
  long long id = eventLoop->timeEventNextId++;
  aeTimeEvent *te;

  te = zmalloc(sizeof(*te));
  if (te == NULL) return AE_ERR;
  te->id = id;
  // 计算超时时间(当前时间 + 给定的超时时间milliseconds)
  aeAddMillisecondsToNow(milliseconds,&te->when_sec,&te->when_ms);
  te->timeProc = proc;
  te->finalizerProc = finalizerProc;
  te->clientData = clientData;

  // 用链表维护多个时间.
  // 采用头插法.
  te->next = eventLoop->timeEventHead;
  eventLoop->timeEventHead = te;
  return id;
}

int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id)
{
  aeTimeEvent *te = eventLoop->timeEventHead;
  while(te) {
    if (te->id == id) {
      te->id = AE_DELETED_EVENT_ID;
      return AE_OK;
    }
    te = te->next;
  }
  return AE_ERR; /* NO event with the specified ID found */
}

/* Search the first timer to fire.
 * This operation is useful to know how many time the select can be
 * put in sleep without to delay any event.
 * If there are no timers NULL is returned.
 *
 * Note that's O(N) since time events are unsorted.
 * Possible optimizations (not needed by Redis so far, but...):
 * 1) Insert the event in order, so that the nearest is just the head.
 *  Much better but still insertion or deletion of timers is O(N).
 * 2) Use a skiplist to have this operation as O(1) and insertion as O(log(N)).
 */
static aeTimeEvent *aeSearchNearestTimer(aeEventLoop *eventLoop)
{
  aeTimeEvent *te = eventLoop->timeEventHead;
  aeTimeEvent *nearest = NULL;

  // 遍历eventset中的时间事件, 找到最近将要发生的事件.
  while (te) {
    if (!nearest || te->when_sec < nearest->when_sec ||
        (te->when_sec == nearest->when_sec && te->when_ms < nearest->when_ms))
      nearest = te;
    te = te->next;
  }
  return nearest;
}

/* Process time events */
static int processTimeEvents(aeEventLoop *eventLoop) {
  int processed = 0;
  aeTimeEvent *te, *prev;
  long long maxId;
  time_t now = time(NULL);

  /* If the system clock is moved to the future, and then set back to the
   * right value, time events may be delayed in a random way. Often this
   * means that scheduled operations will not be performed soon enough.
   *
   * Here we try to detect system clock skews, and force all the time
   * events to be processed ASAP when this happens: the idea is that
   * processing events earlier is less dangerous than delaying them
   * indefinitely, and practice suggests it is. */
  if (now < eventLoop->lastTime) {
    te = eventLoop->timeEventHead;
    while(te) {
      te->when_sec = 0;
      te = te->next;
    }
  }
  eventLoop->lastTime = now;

  prev = NULL;
  te = eventLoop->timeEventHead;
  maxId = eventLoop->timeEventNextId-1;
  while(te) {
    long now_sec, now_ms;
    long long id;

    /* Remove events scheduled for deletion. */
    if (te->id == AE_DELETED_EVENT_ID) {
      aeTimeEvent *next = te->next;
      if (prev == NULL)
        eventLoop->timeEventHead = te->next;
      else
        prev->next = te->next;
      if (te->finalizerProc)
        te->finalizerProc(eventLoop, te->clientData);
      zfree(te);
      te = next;
      continue;
    }

    /* Make sure we don't process time events created by time events in
     * this iteration. Note that this check is currently useless: we always
     * add new timers on the head, however if we change the implementation
     * detail, this check may be useful again: we keep it here for future
     * defense. */
    if (te->id > maxId) {
      te = te->next;
      continue;
    }
    aeGetTime(&now_sec, &now_ms);
    if (now_sec > te->when_sec ||
      (now_sec == te->when_sec && now_ms >= te->when_ms))
    {
      int retval;

      id = te->id;
      // 定时函数timeProc会执行相应的定时事件.
      // 返回值retval表示该定时器事件执行完成后再等待retval继续执行一遍.
      retval = te->timeProc(eventLoop, id, te->clientData);
      processed++;
      if (retval != AE_NOMORE) {
        aeAddMillisecondsToNow(retval,&te->when_sec,&te->when_ms);
      } else {
        te->id = AE_DELETED_EVENT_ID;
      }
    }
    prev = te;
    te = te->next;
  }
  return processed;
}

int aeProcessEvents(aeEventLoop *eventLoop, int flags) {
  int processed = 0, numevents;

  /* Nothing to do? return ASAP */
  if (!(flags & AE_TIME_EVENTS) && !(flags & AE_FILE_EVENTS)) return 0;

  /* Note that we want call select() even if there are no
   * file events to process as long as we want to process time
   * events, in order to sleep until the next time event is ready
   * to fire. */
  if (eventLoop->maxfd != -1 ||  ((flags & AE_TIME_EVENTS) &&
      !(flags & AE_DONT_WAIT))) {
    int j;
    aeTimeEvent *shortest = NULL;
    // tvp 会在 IO 多路复用的函数调用中用到，表示超时时间
    struct timeval tv, *tvp;

    if (flags & AE_TIME_EVENTS && !(flags & AE_DONT_WAIT))
      // 找到一个最近的时间事件. 注意这里是遍历.
      shortest = aeSearchNearestTimer(eventLoop);
    if (shortest) {
      long now_sec, now_ms;

      aeGetTime(&now_sec, &now_ms);
      tvp = &tv;

      /* How many milliseconds we need to wait for the next
       * time event to fire? */
      // 这里主要是根据当前时间戳和最近一个要发生的时间事件里的时间戳
      // 来计算出io复用中需要wait的时间.
      long long ms = (shortest->when_sec - now_sec)*1000 +
        shortest->when_ms - now_ms;

      if (ms > 0) {
        tvp->tv_sec = ms/1000;
        tvp->tv_usec = (ms % 1000)*1000;
      } else {
        tvp->tv_sec = 0;
        tvp->tv_usec = 0;
      }
    } else {
      /* If we have to check for events but need to return
       * ASAP because of AE_DONT_WAIT we need to set the timeout
       * to zero */
      if (flags & AE_DONT_WAIT) {
        tv.tv_sec = tv.tv_usec = 0;
        tvp = &tv;
      } else {
        /* Otherwise we can block */
        tvp = NULL; /* wait forever */
      }
    }

    // 调用io复用接口进行阻塞监听.
    numevents = aeApiPoll(eventLoop, tvp);
    for (j = 0; j < numevents; j++) {
      aeFileEvent *fe = &eventLoop->events[eventLoop->fired[j].fd];
      int mask = eventLoop->fired[j].mask;
      int fd = eventLoop->fired[j].fd;
      int rfired = 0;

      /* note the fe->mask & mask & ... code: maybe an already processed
       * event removed an element that fired and we still didn't
       * processed, so we check if the event is still valid. */
      if (fe->mask & mask & AE_READABLE) {
        rfired = 1;
        fe->rfileProc(eventLoop,fd,fe->clientData,mask);
      }
      if (fe->mask & mask & AE_WRITABLE) {
        if (!rfired || fe->wfileProc != fe->rfileProc)
          fe->wfileProc(eventLoop,fd,fe->clientData,mask);
      }
      processed++;
    }
  }

  // 处理定时器事件.
  if (flags & AE_TIME_EVENTS)
    processed += processTimeEvents(eventLoop);

  return processed; /* return the number of processed file/time events */
}

/* Wait for milliseconds until the given file descriptor becomes
 * writable/readable/exception */
int aeWait(int fd, int mask, long long milliseconds) {
  struct pollfd pfd;
  int retmask = 0, retval;

  memset(&pfd, 0, sizeof(pfd));
  pfd.fd = fd;
  if (mask & AE_READABLE) pfd.events |= POLLIN;
  if (mask & AE_WRITABLE) pfd.events |= POLLOUT;

  if ((retval = poll(&pfd, 1, milliseconds))== 1) {
    if (pfd.revents & POLLIN) retmask |= AE_READABLE;
    if (pfd.revents & POLLOUT) retmask |= AE_WRITABLE;
  if (pfd.revents & POLLERR) retmask |= AE_WRITABLE;
    if (pfd.revents & POLLHUP) retmask |= AE_WRITABLE;
    return retmask;
  } else {
    return retval;
  }
}

void aeMain(aeEventLoop *eventLoop) {
  eventLoop->stop = 0;
  while (!eventLoop->stop) {
    if (eventLoop->beforesleep != NULL)
      eventLoop->beforesleep(eventLoop);

    // AE_ALL_EVENTS表示处理所有的事件.
    aeProcessEvents(eventLoop, AE_ALL_EVENTS);
  }
}

char *aeGetApiName(void) {
  return aeApiName();
}

void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep) {
  eventLoop->beforesleep = beforesleep;
}
