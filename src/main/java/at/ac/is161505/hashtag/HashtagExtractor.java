package at.ac.is161505.hashtag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
public class HashtagExtractor {

    private static final Logger LOGGER = LoggerFactory.getLogger(HashtagExtractor.class);

    private static final Pattern PATTERN = Pattern.compile("(?:\\s|\\A)[##]+([A-Za-z0-9-_]+)");

    public List<String> extract(final String input) {

        final List<String> hashtags = new ArrayList<String>();

        Matcher matcher = PATTERN.matcher(input);
        while (matcher.find()) {
            String hashtag = matcher.group();

            LOGGER.debug("Found hastag: {}", hashtag);
            hashtags.add(hashtag);
        }

        return hashtags;
    }

}
