package com.transport.data.Reader;

import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import org.springframework.batch.item.ReaderNotOpenException;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DefaultFieldSet;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CsvItemReader<T> extends AbstractItemCountingItemStreamItemReader<T>
		implements ResourceAwareItemReaderItemStream<T>, InitializingBean {

	// default encoding for input files
	public static final Charset DEFAULT_CHARSET = Charset.defaultCharset();
	private Resource resource;
	private boolean noInput = false;
	private int lineCount = 0;
	private Charset charset = DEFAULT_CHARSET;
	private int linesToSkip = 0;
	private boolean strict = true;
	private String lineSeparator = "\r\n";
	private char delimiter = ',';
	private char quote = '"';
	private String[] headers;
	private CsvParser csvParser;
	private FieldSetMapper<T> fieldSetMapper;
	private int numberOfKeyValue = 1;

	public CsvItemReader() {
        setName(ClassUtils.getShortName(CsvItemReader.class));
    }

	/**
	 * 読み込み対象のエンコーディングを設定します。デフォルトは {@link #DEFAULT_CHARSET}.
	 *
	 * @param charset 文字コード
	 */
	public void setCharset(Charset charset) {
		this.charset = charset;
	}

	/**
	 * 最初に読み込みをスキップする行数を設定します
	 *
	 * @param linesToSkip the number of lines to skip
	 */
	public void setLinesToSkip(int linesToSkip) {
		this.linesToSkip = linesToSkip;
	}

	/**
	 * strictModeを設定します
	 *
	 * @param strict <code>true</code> by default
	 */
	public void setStrict(boolean strict) {
		this.strict = strict;
	}

	/**
	 * 1行の区切りとなる文字をセットします
	 *
	 * @param lineSeparator 区切り文字（CRLFの場合は\r\n, LFの場合は\n)
	 */
	public void setLineSeparator(String lineSeparator) {
		this.lineSeparator = lineSeparator;
	}

	/**
	 * カラムの区切り文字をセットします
	 *
	 * @param delimiter 区切り文字
	 */
	public void setDelimiter(char delimiter) {
		this.delimiter = delimiter;
	}

	/**
	 * カラムの囲み文字をセットします
	 *
	 * @param quote 囲み文字
	 */
	public void setQuote(char quote) {
		this.quote = quote;
	}

	/**
	 * フィールドのヘッダ情報(Beanのフィールド名で表記)をセットします
	 *
	 * @param headers フィールドのヘッダ情報(Beanのフィールド名で表記)
	 */
	public void setHeaders(String[] headers) {
		this.headers = headers;
	}

	/**
	 * フィールドへのMapperをセットします
	 *
	 * @param fieldSetMapper フィールドへ設定するためのMapper
	 */
	public void setFieldSetMapper(FieldSetMapper<T> fieldSetMapper) {
		this.fieldSetMapper = fieldSetMapper;
	}

	/**
	 * キー項目となる項目番号の個数をセットします（先頭からここで指定された個数の項目をキーとして結果ファイルに出力します）
	 *
	 * @param numberOfKeyValue キー項目の個数
	 */
	public void setNumberOfKeyValue(int numberOfKeyValue) {
		this.numberOfKeyValue = numberOfKeyValue;
	}

	@Override
	public void setResource(Resource resource) {
		this.resource = resource;
	}

	@Override
	protected T doRead() throws Exception {
		if (noInput) {
			return null;
		}

		String[] line = readLine();

		if (line == null) {
			return null;
		}

		// カラム数不一致
		if (line.length != headers.length) {
			// たとえ項目が足りなかったとしてもキー項目のカンマ数は一致させる
			String keyValue = IntStream.range(0, numberOfKeyValue).mapToObj(i -> (line.length > i) ? line[i] : "")
					.collect(Collectors.joining(","));
			throw new Exception(keyValue + " 項目数不一致(" + String.valueOf(line.length) + ")");
		}

		FieldSet fieldSet = new DefaultFieldSet(line, headers);
		return fieldSetMapper.mapFieldSet(fieldSet);
	}

	@Override
	protected void doOpen() throws Exception {
		Assert.notNull(resource, "Input resource must be set");

		noInput = true;
		if (!resource.exists()) {
			if (strict) {
				throw new IllegalStateException("Input resource must exist (reader is in 'strict' mode): " + resource);
			}
			log.warn("Input resource does not exist " + resource.getDescription());
			return;
		}

		if (!resource.isReadable()) {
			if (strict) {
				throw new IllegalStateException(
						"Input resource must be readable (reader is in 'strict' mode): " + resource);
			}
			log.warn("Input resource is not readable " + resource.getDescription());
		}

		csvParser = new CsvParser(settings());
		csvParser.beginParsing(new InputStreamReader(resource.getInputStream(), charset));
		for (int i = 0; i < linesToSkip; i++) {
			readLine();
		}

		noInput = false;
	}

	private CsvParserSettings settings() {
		CsvParserSettings settings = new CsvParserSettings();
		settings.getFormat().setLineSeparator(lineSeparator);
		settings.getFormat().setDelimiter(delimiter);
		settings.getFormat().setQuote(quote);
		settings.setEmptyValue("");
		return settings;
	} 
	
	private String[] readLine() {
		if (csvParser == null) {
			throw new ReaderNotOpenException("Parser must be open before it can be read");
		}

		String[] line = csvParser.parseNext();
		if (line == null) {
			return null;
		}
		lineCount++;

		return line;
	}

	@Override
	protected void doClose() throws Exception {
		lineCount = 0;
		if (csvParser != null) {
			csvParser.stopParsing();
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(headers, "headers is required");
		Assert.notNull(fieldSetMapper, "FieldSetMapper is required");
	}
}