using System;

namespace AzureFuncApp
{
    public static class Utils
    {
        public static string FormatDate(string input)
        {
            if (string.IsNullOrWhiteSpace(input))
            {
                return null;
            }

            return DateTime.Parse(input).ToString("yyyy-MM-dd HH:mm:ss");
        }

        public static string RemoveNewline(string input)
        {
            if (string.IsNullOrWhiteSpace(input))
            {
                return input;
            }

            return input.Trim().Replace("\n", "");
        }
    }
}
