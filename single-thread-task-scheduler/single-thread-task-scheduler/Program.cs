using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace single_thread_task_scheduler
{
    class Program
    {
        static DedicatedThreadScheduler backgroundWorkerScheduler = new DedicatedThreadScheduler();
        static CountdownEvent helloWorld;
        static void Main(string[] args)
        {
            var helloWorldCount = 5;
            var helloWorldInlineQueuedCount = 5;
            var helloWorldInlineNonQueuedCount = 5;

            helloWorld = new CountdownEvent(helloWorldCount+helloWorldInlineNonQueuedCount+helloWorldInlineQueuedCount);

            for (var i = 0; i < helloWorldCount; i++)
            {
                RunTask(HelloWorld);
            }

            for (var i = 0; i < helloWorldInlineQueuedCount; i++)
            {
                RunTask(HelloWorldInlineQueued);
            }

            for (var i = 0; i < helloWorldInlineNonQueuedCount; i++)
            {
                RunTask(HelloWorldInlineNotQueued);
            }

            helloWorld.Wait();
            Console.WriteLine("Done!");
            Console.ReadKey();
        }

        static void HelloWorld()
        {
            Thread.Sleep(1000);

            Console.WriteLine("Hello World!");

            helloWorld.Signal();
        }

        static void HelloWorldInlineQueued()
        {
            RunTask(HelloWorld).Wait();
        }

        static void HelloWorldInlineNotQueued()
        {
            new Task(HelloWorld, TaskCreationOptions.None).RunSynchronously(backgroundWorkerScheduler);
        }

        static Task RunTask(Action action)
        {
            return Task.Factory.StartNew(action, CancellationToken.None, TaskCreationOptions.None, backgroundWorkerScheduler);
        }

        class DedicatedThreadScheduler : TaskScheduler
        {
            LinkedList<Task> _taskQueue = new LinkedList<Task>();
            ManualResetEvent _workRequired = new ManualResetEvent(false);
            Thread _thread;

            public DedicatedThreadScheduler()
            {
                _thread = new Thread(RunWorker);

                _thread.Start();
            }

            private void RunWorker(object obj)
            {
                while (true)
                {
                    _workRequired.WaitOne();

                    if (base.TryExecuteTask(_taskQueue.Last.Value))
                    {
                        DequeueTask(_taskQueue.Last);
                    }
                }
            }

            protected override IEnumerable<Task> GetScheduledTasks()
            {
                // this is for debugger support only

                return _taskQueue;
            }


            protected override void QueueTask(Task task)
            {
                Trace.WriteLine("Scheduler: queuing task");

                lock (_taskQueue)
                {
                    _taskQueue.AddFirst(task);
                    _workRequired.Set();
                }
            }

            void DequeueTask(LinkedListNode<Task> task) 
            {
                lock (_taskQueue)
                {
                    _taskQueue.Remove(task);

                    if (_taskQueue.Count == 0)
                    {
                        Trace.WriteLine("Scheduler: pausing worker as task queue is empty");
                        _workRequired.Reset();
                    }
                }
            }

            void DequeueTask(Task task)
            {
                lock (_taskQueue)
                {
                    _taskQueue.Remove(task);

                    if (_taskQueue.Count == 0)
                    {
                        Trace.WriteLine("Scheduler: pausing worker as task queue is empty");
                        _workRequired.Reset();
                    }
                }
            }

            protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
            {
                // Task.RunSynchronously will cause TryExecuteTaskInline with false
                // Task.StartNew(..).Wait() can cause TryExecuteTaskInline with true

                // if a task is started then waited on then why not run it on the same thread. the scheduler is the class that 
                // gets to choose whether to inline or not via TryExecuteTaskInline

                Trace.WriteLine("Scheduler: attempting to inline task");

                if (Thread.CurrentThread != _thread)
                {
                    Trace.WriteLine("Scheduler: rejected as current thread is not worker");
                }

                if (taskWasPreviouslyQueued)
                {
                    Trace.WriteLine("Scheduler: task being removed from queue");
                    DequeueTask(task);
                    Trace.WriteLine("Scheduler: task removed from queue");
                }
                else
                {
                    Trace.WriteLine("Scheduler: task not in queue");
                }

                Trace.WriteLine("Scheduler: trying to execute task");
                var executed = TryExecuteTask(task);

                if (executed)
                {
                    Trace.WriteLine("Scheduler: task executed");
                } 
                else
                {
                    Trace.WriteLine("Scheduler: task execution failed");
                }

                return executed;
            }
        }
    }
}
