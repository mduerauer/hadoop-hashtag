package at.ac.is161505.hastag;

import at.ac.is161505.hashtag.HashtagExtractor;
import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * This file is part of hashtag.
 * <p>
 * hashtag is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p>
 * hashtag is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * Diese Datei ist Teil von hashtag.
 * <p>
 * hashtag ist Freie Software: Sie können es unter den Bedingungen
 * der GNU General Public License, wie von der Free Software Foundation,
 * Version 3 der Lizenz oder (nach Ihrer Wahl) jeder späteren
 * veröffentlichten Version, weiterverbreiten und/oder modifizieren.
 * <p>
 * hashtag wird in der Hoffnung, dass es nützlich sein wird, aber
 * OHNE JEDE GEWÄHRLEISTUNG, bereitgestellt; sogar ohne die implizite
 * Gewährleistung der MARKTFÄHIGKEIT oder EIGNUNG FÜR EINEN BESTIMMTEN ZWECK.
 * Siehe die GNU General Public License für weitere Details.
 * <p>
 * Sie sollten eine Kopie der GNU General Public License zusammen mit diesem
 * Programm erhalten haben. Wenn nicht, siehe <http://www.gnu.org/licenses/>.
 * <p>
 * Created by n17405180 on 21.10.17.
 */
public class HashtagExtractorTests {

    private static final Logger LOGGER = LoggerFactory.getLogger(HashtagExtractorTests.class);

    private HashtagExtractor hashtagExtractor;

    public static String TEST_INPUT1 = "Hallo #world, das ist ein #test, um #hashtags aus einem Text zu extrahieren.";

    @Before
    public void init() {
        hashtagExtractor = new HashtagExtractor();
    }

    @Test
    public void shouldExtractHashtags() {
        List<String> extractedHashtags = hashtagExtractor.extract(TEST_INPUT1);

        assertNotNull(extractedHashtags);

        for(String hashTag : extractedHashtags) {
            LOGGER.debug("Found hashtag {}", hashTag);
            assertFalse(StringUtils.isEmpty(hashTag));
        }

        assertTrue(extractedHashtags.size() == 3);

    }

}
