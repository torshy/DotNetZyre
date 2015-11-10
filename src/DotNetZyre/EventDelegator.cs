using System;
using System.Threading;

namespace DotNetZyre
{
    /// <summary>
    /// Facilitates a pattern whereby an event may be decorated with logic that transforms its arguments.
    /// </summary>
    /// <remarks>
    /// Use of this class requires providing actions that register and unregister a handler of the source
    /// event that calls <see cref="Fire"/> with updated arguments in response.
    /// </remarks>
    /// <typeparam name="T">Argument type of the decorated event.</typeparam>
    internal class EventDelegator<T> : IDisposable where T : EventArgs
    {
        private readonly Action _registerToEvent;
        private readonly Action _unregisterFromEvent;
        private EventHandler<T> _event;
        private int _counter;

        /// <summary>
        /// Initialises a new instance.
        /// </summary>
        /// <param name="registerToEvent">an Action to perform when the first handler is registered for the event</param>
        /// <param name="unregisterFromEvent">an Action to perform when the last handler is unregistered from the event</param>
        public EventDelegator(Action registerToEvent, Action unregisterFromEvent)
        {
            _registerToEvent = registerToEvent;
            _unregisterFromEvent = unregisterFromEvent;
        }

        public event EventHandler<T> Event
        {
            add
            {
                _event += value;

                if (Interlocked.Increment(ref _counter) == 1)
                {
                    _registerToEvent();
                }
            }
            remove
            {
                _event -= value;

                if (Interlocked.Decrement(ref _counter) == 0)
                {
                    _unregisterFromEvent();
                }
            }
        }

        /// <summary>
        /// Raise, or "Fire", the Event.
        /// </summary>
        /// <param name="sender">the sender that the event-handler that gets notified of this event will receive</param>
        /// <param name="args">the subclass of EventArgs that the event-handler will receive</param>
        public void Fire(object sender, T args)
        {
            var temp = _event;
            if (temp != null)
            {
                temp(sender, args);
            }
        }

        public void Dispose()
        {
            if (_counter != 0)
            {
                _unregisterFromEvent();
                _counter = 0;
            }
        }
    }
}