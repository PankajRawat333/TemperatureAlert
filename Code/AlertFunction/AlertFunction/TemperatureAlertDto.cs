using System;
using System.Collections.Generic;
using System.Text;

namespace AlertFunction
{
    public class TemperatureAlertDto
    {
        public string DeviceId { get; set; }
        public string EventProcessedUtcTime { get; set; }
    }
}
