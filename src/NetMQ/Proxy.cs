using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NetMQ.Sockets;

namespace NetMQ
{
    /// <summary>
    /// Forward messages between two sockets, you can also specify control socket which both sockets will send messages to
    /// </summary>
    public class Proxy
    {
        NetMQSocket m_frontend;
        NetMQSocket m_backend;
        NetMQSocket m_controlIn;
        NetMQSocket m_controlOut;

        public Proxy(NetMQSocket frontend, NetMQSocket backend, NetMQSocket controlIn, NetMQSocket controlOut = null)
        {
            m_frontend = frontend;
            m_backend = backend;
            m_controlIn = controlIn;
            m_controlOut = controlOut;
        }

        /// <summary>
        /// Start the proxy work, this will block until one of the sockets is closed
        /// </summary>
        public void Start()
        {
            zmq.ZMQ.Proxy(m_frontend.SocketHandle, 
                          m_backend.SocketHandle, 
                          m_controlIn != null ? m_controlIn.SocketHandle : null, 
                          m_controlOut != null ? m_controlOut.SocketHandle : null);
        }
    }
}
