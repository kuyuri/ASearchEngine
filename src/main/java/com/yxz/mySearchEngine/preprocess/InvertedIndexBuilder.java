package com.yxz.mySearchEngine.preprocess;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chenlb.mmseg4j.ComplexSeg;
import com.chenlb.mmseg4j.Dictionary;
import com.chenlb.mmseg4j.MMSeg;
import com.chenlb.mmseg4j.Seg;
import com.chenlb.mmseg4j.Word;

/**
* @author Yu 
* 构建倒排索引
*/
public class InvertedIndexBuilder {

	private static final Logger logger = LoggerFactory.getLogger(InvertedIndexBuilder.class);
	
	private Map<Integer, PostingEntry> invertedIndexMap = new HashMap<>();
	
	public void addDocument(int documentId, String content) {
		updateInvertedIndexMap(documentId, content); //更新内存中的倒排索引
		if(indexNeedMerge()) { //内存中的倒排索引过大，则和磁盘中的倒排索引合并
			updateInvertedIndex();
		}
	}

	private void updateInvertedIndex() {
		
	}

	private boolean indexNeedMerge() {
		return false;
	}

	private void updateInvertedIndexMap(int documentId, String content) {
		Dictionary dic = Dictionary.getInstance();
		Seg seg = new ComplexSeg(dic);
		MMSeg mmSeg = new MMSeg(new StringReader(content), seg);
		Word word = null;
		try {
			while ((word = mmSeg.next()) != null) { //使用专业分词工具进行分词
				String token = word.toString();
				int offset = word.getStartOffset();
				int tokenId = getTokenId(token);
				PostingEntry postingEntry = invertedIndexMap.get(tokenId);
				Entry entry = null;
				if(postingEntry != null) { //定位Entry
					List<Entry> list = postingEntry.getEntries();
					for(Entry e : list) {
						if(e.getDocumentId() == documentId) {
							entry = e;
							break;
						}
					}
				}
				else { //新建Entry并增加tokenId
					postingEntry = new PostingEntry();
					postingEntry.getEntries().add(new Entry(documentId));
					postingEntry.getEntries().get(0).getPositions().add(offset);
					continue;
				}
				if(entry != null) { //增加tokenId
					entry.getPositions().add(offset);
				}
				else { //词元出现过，但document第一次出现
					postingEntry.getEntries().add(new Entry(documentId));
					List<Entry> list = postingEntry.getEntries();
					list.get(list.size() - 1).getPositions().add(offset);
				}
			}
		} catch (IOException e) {
			logger.error("Document :" + documentId + " was failed to get tokens", e);
		}
	}
	
	private int getTokenId(String token) {
		// TODO Auto-generated method stub
		return 0;
	}

	public static void main(String[] args) throws Exception {

	}
}
