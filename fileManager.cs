using System;
using System.Collections.Generic;
using System.Text;

namespace COLT
{
    public static class fileManager
    {

        public static byte[] generateRandomBytes(int sizeInBytes)
        {
            byte[] array = new byte[sizeInBytes];
            Random random = new Random();
            random.NextBytes(array);
            return array;
        }

        

    }
}
