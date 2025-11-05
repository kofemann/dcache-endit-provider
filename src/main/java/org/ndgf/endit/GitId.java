/* dCache Endit Nearline Storage Provider
 *
 * * Copyright (C) 2025 Niklas Edmundsson
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.ndgf.endit;

import java.io.IOException;
import java.util.Properties;

public final class GitId {
    public static final String DESCRIBE;

    static {
        try {
            Properties properties = new Properties();
            properties.load(GitId.class.getClassLoader().getResourceAsStream("dcache-endit-provider-git.properties"));
            DESCRIBE = String.valueOf(properties.get("git.commit.id.describe"));

        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

private GitId() {}
}
