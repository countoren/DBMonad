using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TMDBConnector
{
 /// <summary>
    /// A unit type is a type that allows only one value (and thus can hold no information)
    /// </summary>
    [Serializable]
    public struct Unit : IEquatable<Unit>, IComparable<Unit>
    {
        public static readonly Unit Default = new Unit();

        [Pure]
        public override int GetHashCode()
        {
            return 0;
        }

        [Pure]
        public override bool Equals(object obj)
        {
            return obj is Unit;
        }

        [Pure]
        public override string ToString()
        {
            return "()";
        }

        [Pure]
        public bool Equals(Unit other)
        {
            return true;
        }

        /// <summary>
        /// Provide an alternative value to unit
        /// </summary>
        /// <typeparam name="T">Alternative value type</typeparam>
        /// <param name="anything">Alternative value</param>
        /// <returns>Alternative value</returns>
        [Pure]
        public T Return<T>(T anything)
        {
            return anything;
        }

        /// <summary>
        /// Provide an alternative value to unit
        /// </summary>
        /// <typeparam name="T">Alternative value type</typeparam>
        /// <param name="anything">Alternative value</param>
        /// <returns>Alternative value</returns>
        [Pure]
        public T Return<T>(Func<T> anything)
        {
            return anything();
        }

        /// <summary>
        /// Always equal
        /// </summary>
        [Pure]
        public int CompareTo(Unit other)
        {
            return 0;
        }
    }

        public static class Prelude
        {
            /// <summary>
            /// Unit constructor
            /// </summary>
            public static Unit unit = Unit.Default;

            /// <summary>
            /// Takes any value, ignores it, returns a unit
            /// </summary>
            /// <param name="anything">Value to ignore</param>
            /// <returns>Unit</returns>
            public static Unit ignore<T>(T anything)
            {
                return unit;
            }
        }
}
