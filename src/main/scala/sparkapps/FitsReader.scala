package sparkapps

import nom.tam.fits._

object  FitsReader {

  def fitsJob() {

    val resourceFolder = System.getProperty("user.dir")+"/src/main/resources/sparkapps/"
    val FitsFile = resourceFolder + "test.fits"

    val hdu = new Fits(FitsFile).getHDU(0)

    val header = hdu.getHeader()
    val pixelScale = header.getFloatValue("CD1_1")
    println(s"Pixel scale: ${pixelScale * 3600} arcsec")

    val size = hdu.getAxes()
    println(s"Image size: (${size(0)}, ${size(1)}) pixels")

    val data = hdu.getKernel().asInstanceOf[Array[Array[Double]]]
    println(s"Value of central pixel: ${data(255)(255)}")

  }

  def main(args: Array[String]) = fitsJob()
}
