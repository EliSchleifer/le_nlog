// 
// Copyright (c) 2010-2012 Logentries, Jlizard
// 
// All rights reserved.
// 
// Redistribution and use in source and binary forms, with or without 
// modification, are permitted provided that the following conditions 
// are met:
// 
// * Redistributions of source code must retain the above copyright notice, 
//   this list of conditions and the following disclaimer. 
// 
// * Redistributions in binary form must reproduce the above copyright notice,
//   this list of conditions and the following disclaimer in the documentation
//   and/or other materials provided with the distribution. 
// 
// * Neither the name of Logentries nor the names of its 
//   contributors may be used to endorse or promote products derived from this
//   software without specific prior written permission. 
// 
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE 
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF 
// THE POSSIBILITY OF SUCH DAMAGE.
// 
// Mark Lacomber <marklacomber@gmail.com>
// Viliam Holub <vilda@logentries.com>

using System;
using System.Configuration;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Web;
using System.Net.Security;
using System.Net.Sockets;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;

using NLog;
using NLog.Common;
using NLog.Config;
using NLog.Internal;
using NLog.Internal.NetworkSenders;
using NLog.Layouts;
using NLog.Targets;
using System.Text;

namespace NLog.Targets
{
    [Target("Logentries")]
    public sealed class LogEntriesTarget : TargetWithLayout
    {
        /*
         * Constants
         */

        /** Size of the internal event queue. */
        public static readonly int QUEUE_SIZE = 32768;
        /** Logentries API server address. */
        private const string LogEntriesUri = "api.logentries.com";
        /** Port number for token logging on Logentries API server. */
        private const int LE_PORT = 10000;
        /** UTF-8 output character set. */
        private static readonly UTF8Encoding UTF8 = new UTF8Encoding();
        /** ASCII character set used by HTTP. */
        private static readonly ASCIIEncoding ASCII = new ASCIIEncoding();
        /** Minimal delay between attempts to reconnect in milliseconds. */
        private const int MIN_DELAY = 100;
        /** Maximal delay between attempts to reconnect in milliseconds. */
        private const int MAX_DELAY = 10000;
        /** LE appender signature - used for debugging messages. */
        private const string LE = "LE: ";
        /** Logentries Config Key */
        private const string CONFIG_TOKEN = "LOGENTRIES_TOKEN";
        /** Error message displayed when invalid token is detected. */
        private const string INVALID_TOKEN = "\n\nIt appears your LOGENTRIES_TOKEN parameter in web/app.config is invalid!\n\n";

        private readonly Random random = new Random();

        //Custom socket class to allow for choice of SSL
        private TcpClient client = null;
        private Stream socket = null;
        public Thread thread;
        public bool started = false;
        private String token = "SET_TOKEN_TO_LOG_TO_SERVICE";
        /** Message Queue. */
        public BlockingCollection<byte[]> queue;

        public LogEntriesTarget()
            : this(null)
        {
            var settings = ConfigurationManager.AppSettings;
            if (settings.HasKeys() && settings.AllKeys.Contains(CONFIG_TOKEN))
            {
                this.token = settings[CONFIG_TOKEN];
            }
            else
            {
                WriteToDebug("Configuration missing LOGENTRIES_TOKEN value, logger will not work");
            }
        }

        public LogEntriesTarget(string token)
        {
            queue = new BlockingCollection<byte[]>(QUEUE_SIZE);

            thread = new Thread(new ThreadStart(RunLoop));
            thread.Name = "Logentries NLog Target";
            thread.IsBackground = true;

            this.token = token;
        }

        /** Debug flag. */
        [RequiredParameter]
        public bool Debug { get; set; }

        private void OpenConnection()
        {
            try
            {
                this.client = new TcpClient(LogEntriesUri, LE_PORT);
                this.client.NoDelay = true;

                this.socket = this.client.GetStream();
            }
            catch
            {
                throw new IOException();
            }
        }

        private void ReopenConnection()
        {
            CloseConnection();

            int root_delay = MIN_DELAY;
            while (true)
            {
                try
                {
                    OpenConnection();
                    return;
                }
                catch (Exception e)
                {
                    if (Debug)
                    {
                        WriteToDebug("Unable to connect to Logentries");
                        WriteToDebug(e.ToString());
                    }
                }

                root_delay *= 2;
                if (root_delay > MAX_DELAY)
                    root_delay = MAX_DELAY;
                int wait_for = root_delay + random.Next(root_delay);

                try
                {
                    Thread.Sleep(wait_for);
                }
                catch
                {
                    throw new ThreadInterruptedException();
                }
            }
        }

        private void CloseConnection()
        {
            if (this.client != null)
            {
                this.client.Close();
            }
        }

        public void RunLoop()
        {
            try
            {
                // Open connection
                ReopenConnection();

                // Send data in queue
                while (true)
                {

                    // Block until data shows up in the queue
                    byte[] data = queue.Take();
                    int count = queue.Count;

                    while (count > 0)
                    {
                        Write(data);
                        data = queue.Take();
                        count--;
                    }
                    Flush();
                }
            }
            catch (ThreadInterruptedException e)
            {
                WriteToDebug("Logentries asynchronous socket interrupted");
            }

            CloseConnection();
        }

        /// <summary>
        /// Reliably write bytes to service
        /// </summary>
        /// <param name="data"></param>
        private void Write(byte[] data)
        {
            while (true)
            {
                try
                {
                    this.socket.Write(data, 0, data.Length);
                    data = queue.Take();
                }
                catch (IOException)
                {
                    // Reopen the lost connection
                    ReopenConnection();
                    continue;
                }
                break;
            }
        }

        /// <summary>
        /// Flush pipe to service
        /// </summary>
        private void Flush()
        {
            try
            {
                this.socket.Flush();
            }
            catch (IOException)
            {
                ReopenConnection();
            }
        }

        private void EnqueueLine(string line)
        {
            WriteToDebug("Queueing " + line);

            byte[] data = UTF8.GetBytes(this.token + line + '\n');

            //Try to append data to queue
            bool isFull = !queue.TryAdd(data);

            //If it's full, remove latest item and try again
            if (isFull)
            {
                queue.Take();
                queue.TryAdd(data);
            }
        }

        private bool checkCredentials()
        {
            if (string.IsNullOrEmpty(this.token))
            {
                return false;
            }

            Guid guid;
            if (!Guid.TryParse(this.token, out guid))
            {
                WriteToDebug(INVALID_TOKEN);
                return false;
            }
            return true;
        }

        protected override void Write(LogEventInfo logEvent)
        {
            if (!checkCredentials())
            {
                WriteToDebug(INVALID_TOKEN);
                return;
            }
            if (!started)
            {
                WriteToDebug("Starting Logentries asynchronous socket client");
                thread.Start();
                started = true;
            }

            //Append message content
            EnqueueLine(this.Layout.Render(logEvent));

            // Write out exception (this should really be using the exception formatter)
            if (logEvent.Exception != null)
            {
                var str = logEvent.Exception.ToString();
                str = str.Replace('\n', '\u2028');
                EnqueueLine(str);
            }
        }

        protected override void CloseTarget()
        {
            base.CloseTarget();
            thread.Interrupt();
        }

        private void WriteToDebug(string message)
        {
            if (!this.Debug) { return; }

            message = LE + message;
            System.Diagnostics.Debug.WriteLine(message);
            Console.Error.WriteLine(message);
            //Log to NLog's internal logger also
            InternalLogger.Debug(message);
        }
    }
}
