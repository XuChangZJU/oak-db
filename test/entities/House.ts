import { String, Int, Datetime, Image, Boolean, Text, Float  } from 'oak-domain/lib/types/DataType';
import { Schema as Area } from 'oak-general-business/lib/entities/Area';
import { Schema as User } from 'oak-general-business/lib/entities/User';
import { Schema as ExtraFile } from 'oak-general-business/lib/entities/ExtraFile';
import { EntityShape } from 'oak-domain/lib/types';
import { LocaleDef } from 'oak-domain/lib/types/Locale';

export interface Schema extends EntityShape {
    district: String<16>;
    area: Area;
    owner: User;
    dd: Array<ExtraFile>;
    size: Float<4, 2>;
};

const locale: LocaleDef<Schema, '', '', {}> = {
    zh_CN: {
        name: '房屋',
        attr: {
            district: '街区',
            area: '地区',
            owner: '房主',
            dd: '文件',
            size: '面积',
        },
    },
};