using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace AzureFuncApp
{
    public static class ExtensionMethods
    {
        public static IEnumerable<string> ToOutputFormat(this IEnumerable<Dictionary<string, string>> dataItems, IEnumerable<string> fieldNames, string quote = "\"", string delimiter = "|", bool includeHeader = true)
        {
            string lastField = fieldNames.Last();

            if (includeHeader)
            {
                yield return string.Join(delimiter, fieldNames);
            }

            foreach (var item in dataItems)
            {
                var sb = new StringBuilder();
                foreach (var key in fieldNames)
                {
                    sb.Append(quote + (item[key] ?? string.Empty) + quote);
                    if (key != lastField)
                    {
                        sb.Append(delimiter);
                    }
                }

                yield return sb.ToString();
            }
        }

        public static IEnumerable<Dictionary<string, string>> ProcessDateFields(this IEnumerable<Dictionary<string, string>> dataItems, IEnumerable<string> fieldNames)
        {
            foreach (var item in dataItems)
            {
                foreach (var key in fieldNames)
                {
                    item[key] = Utils.FormatDate(item[key]);
                }

                yield return item;
            }
        }

        public static IEnumerable<Dictionary<string, string>> RemoveNewlineCharacters(this IEnumerable<Dictionary<string, string>> dataItems, IEnumerable<string> fieldNames)
        {
            foreach (var item in dataItems)
            {
                foreach (var key in fieldNames)
                {
                    item[key] = Utils.RemoveNewline(item[key]);
                }

                yield return item;
            }
        }
    }
}
