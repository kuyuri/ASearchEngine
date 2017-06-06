package com.yxz.mySearchEngine.preprocess;

import java.util.ArrayList;
import java.util.List;

/**
* @author Yu 
* 倒排列表项
*/
public class PostingEntry {
	
	private List<Entry> entries = new ArrayList<>();

	public List<Entry> getEntries() {
		return entries;
	}
	public void setEntries(List<Entry> entries) {
		this.entries = entries;
	}
	
}

class Entry {
	
	private int documentId;  //文档编号
	private List<Integer> positions; //词元出现的位置
	
	public Entry(int documentId) {
		this.documentId = documentId;
		this.positions = new ArrayList<>();
	}
	
	public int getDocumentId() {
		return documentId;
	}
	public void setDocumentId(int documentId) {
		this.documentId = documentId;
	}
	public List<Integer> getPositions() {
		return positions;
	}
	public void setPositions(List<Integer> positions) {
		this.positions = positions;
	}
	
}