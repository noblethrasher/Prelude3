using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Prelude
{
    public struct NonEmptyString
    {
        readonly string s;

        public NonEmptyString(string s) => this.s = s;

        public bool IsValid => IsValidString(s);

        static bool IsValidString(string s) => s?.Length > 0;

        public static bool TryParse(string s, out NonEmptyString ns) => (ns = IsValidString(s) ? new NonEmptyString(s) : default(NonEmptyString)).IsValid;

        public static implicit operator string (NonEmptyString s) => s.IsValid ? s.s : throw new InvalidOperationException("string is null or empty");
    }

    public static class StringUtils
    {
        //Courtesy of Wikibooks: https://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Levenshtein_distance#C.23 retreived on 9/19/2017
        public static int NormalizedLevenshtein(this string a, string b)
        {
            a = a.ToUpper();
            b = b.ToUpper();

            bool EmptyTest(string x, string y, out int? length)
            {
                if (string.IsNullOrEmpty(x))
                {
                    length = (!string.IsNullOrEmpty(y)) ? y.Length : 0;

                    return true;
                }

                length = null;

                return false;
            }

            if (EmptyTest(a, b, out var len)) return len.Value;

            if (EmptyTest(b, a, out len)) return len.Value;

            var d = new int[a.Length + 1, b.Length + 1];

            for (int i = 0; i <= d.GetUpperBound(0); i++)
                d[i, 0] = i;

            for (int i = 0; i <= d.GetUpperBound(1); i++)
                d[0, i] = i;

            for (int i = 1; i <= d.GetUpperBound(0); i++)
            {
                for (int j = 1; j <= d.GetUpperBound(1); j++)
                {
                    var cost = Convert.ToInt32(!(a[i - 1] == b[j - 1]));

                    var min1 = d[i - 1, j] + 1;
                    var min2 = d[i, j - 1] + 1;
                    var min3 = d[i - 1, j - 1] + cost;

                    d[i, j] = Math.Min(Math.Min(min1, min2), min3);
                }
            }

            return d[d.GetUpperBound(0), d.GetUpperBound(1)];
        }
    }
}