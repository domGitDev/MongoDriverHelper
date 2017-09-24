using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Foundation;
using UIKit;

namespace TestApp.iOS
{
    class FileAccessHelper
    {
        public static string GetLocalFilePath(string filename, bool isDirectory=false)
        {
            string docFolder = Environment.GetFolderPath(Environment.SpecialFolder.Personal);
            string libFolder = System.IO.Path.GetFullPath(System.IO.Path.Combine(docFolder, "..", "Library"));

            if (!System.IO.Directory.Exists(libFolder))
            {
                System.IO.Directory.CreateDirectory(libFolder);
            }
            string filepath = System.IO.Path.Combine(libFolder, filename);
            if (!System.IO.Directory.Exists(filepath) && isDirectory)
                    System.IO.Directory.CreateDirectory(filepath);
            
            return filepath;
        }
        
        public static string MediaTempFilename(string dirname, string ext = "jpg")
        {
            var dataPath = FileAccessHelper.GetLocalFilePath(dirname, true);
            var filename = Path.GetFileName(Path.GetTempFileName());
            return Path.Combine(dataPath, string.Format("{0}.{1}", filename, ext.Replace(".", "")));
        }
    }
}
