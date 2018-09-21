using System;
using System.Collections.Generic;
using System.Text;

namespace COLT
{
    public class TimedEvent
    {

        public long elapsedMiliseconds { get; set; }
        public string eventType { get; set; }
        public string cloudName { get; set; }    
        public string cloudType { get; set; }
        public string fileName { get; set; }
        public long fileSizeInBytes { get; set; }
        public DateTime startTime { get; set; }
        public DateTime finishTime { get; set; }

        public string url { get; set; }


    }
}
