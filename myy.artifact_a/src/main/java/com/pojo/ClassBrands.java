package com.pojo;

import java.io.Serializable;


public class ClassBrands implements Serializable {
	
	private static final long serialVersionUID = 1L;
	public String SubBrand;
	public String BrandName;
	public String CatLib;
	public String ProdKey;
	public String CatLibProdKey;

	
	
	public String getSubBrand() {
		return SubBrand;
	}



	public void setSubBrand(String subBrand) {
		SubBrand = subBrand;
	}



	public String getBrandName() {
		return BrandName;
	}



	public void setBrandName(String brandName) {
		BrandName = brandName;
	}



	public String getCatLib() {
		return CatLib;
	}



	public void setCatLib(String catLib) {
		CatLib = catLib;
	}



	public String getProdKey() {
		return ProdKey;
	}



	public void setProdKey(String prodKey) {
		ProdKey = prodKey;
	}



	public String getCatLibProdKey() {
		return CatLibProdKey;
	}



	public void setCatLibProdKey(String catLibProdKey) {
		CatLibProdKey = catLibProdKey;
	}



	public String toString() {
		return "SubBrand:" + SubBrand + " BrandName:" + BrandName + " CatLib:" + CatLib + "ProdKey:" + ProdKey + "CatLibProdKey:"
				+ CatLibProdKey;
	}
	
	
}
