#pragma once

namespace ylt::reflection {
[[noreturn]] inline void unreachable() {
  // Uses compiler specific extensions if possible.
  // Even if no extension is used, undefined behavior is still raised by
  // an empty function body and the noreturn attribute.
#ifdef __GNUC__  // GCC, Clang, ICC
  __builtin_unreachable();
#elif defined(_MSC_VER)  // msvc
  __assume(false);
#endif
}

template <typename Func, typename... Args>
constexpr decltype(auto) inline template_switch(size_t index, Args &&...args) {
  switch (index) {
    case 0:
      return Func::template run<0>(std::forward<Args>(args)...);
    case 1:
      return Func::template run<1>(std::forward<Args>(args)...);
    case 2:
      return Func::template run<2>(std::forward<Args>(args)...);
    case 3:
      return Func::template run<3>(std::forward<Args>(args)...);
    case 4:
      return Func::template run<4>(std::forward<Args>(args)...);
    case 5:
      return Func::template run<5>(std::forward<Args>(args)...);
    case 6:
      return Func::template run<6>(std::forward<Args>(args)...);
    case 7:
      return Func::template run<7>(std::forward<Args>(args)...);
    case 8:
      return Func::template run<8>(std::forward<Args>(args)...);
    case 9:
      return Func::template run<9>(std::forward<Args>(args)...);
    case 10:
      return Func::template run<10>(std::forward<Args>(args)...);
    case 11:
      return Func::template run<11>(std::forward<Args>(args)...);
    case 12:
      return Func::template run<12>(std::forward<Args>(args)...);
    case 13:
      return Func::template run<13>(std::forward<Args>(args)...);
    case 14:
      return Func::template run<14>(std::forward<Args>(args)...);
    case 15:
      return Func::template run<15>(std::forward<Args>(args)...);
    case 16:
      return Func::template run<16>(std::forward<Args>(args)...);
    case 17:
      return Func::template run<17>(std::forward<Args>(args)...);
    case 18:
      return Func::template run<18>(std::forward<Args>(args)...);
    case 19:
      return Func::template run<19>(std::forward<Args>(args)...);
    case 20:
      return Func::template run<20>(std::forward<Args>(args)...);
    case 21:
      return Func::template run<21>(std::forward<Args>(args)...);
    case 22:
      return Func::template run<22>(std::forward<Args>(args)...);
    case 23:
      return Func::template run<23>(std::forward<Args>(args)...);
    case 24:
      return Func::template run<24>(std::forward<Args>(args)...);
    case 25:
      return Func::template run<25>(std::forward<Args>(args)...);
    case 26:
      return Func::template run<26>(std::forward<Args>(args)...);
    case 27:
      return Func::template run<27>(std::forward<Args>(args)...);
    case 28:
      return Func::template run<28>(std::forward<Args>(args)...);
    case 29:
      return Func::template run<29>(std::forward<Args>(args)...);
    case 30:
      return Func::template run<30>(std::forward<Args>(args)...);
    case 31:
      return Func::template run<31>(std::forward<Args>(args)...);
    case 32:
      return Func::template run<32>(std::forward<Args>(args)...);
    case 33:
      return Func::template run<33>(std::forward<Args>(args)...);
    case 34:
      return Func::template run<34>(std::forward<Args>(args)...);
    case 35:
      return Func::template run<35>(std::forward<Args>(args)...);
    case 36:
      return Func::template run<36>(std::forward<Args>(args)...);
    case 37:
      return Func::template run<37>(std::forward<Args>(args)...);
    case 38:
      return Func::template run<38>(std::forward<Args>(args)...);
    case 39:
      return Func::template run<39>(std::forward<Args>(args)...);
    case 40:
      return Func::template run<40>(std::forward<Args>(args)...);
    case 41:
      return Func::template run<41>(std::forward<Args>(args)...);
    case 42:
      return Func::template run<42>(std::forward<Args>(args)...);
    case 43:
      return Func::template run<43>(std::forward<Args>(args)...);
    case 44:
      return Func::template run<44>(std::forward<Args>(args)...);
    case 45:
      return Func::template run<45>(std::forward<Args>(args)...);
    case 46:
      return Func::template run<46>(std::forward<Args>(args)...);
    case 47:
      return Func::template run<47>(std::forward<Args>(args)...);
    case 48:
      return Func::template run<48>(std::forward<Args>(args)...);
    case 49:
      return Func::template run<49>(std::forward<Args>(args)...);
    case 50:
      return Func::template run<50>(std::forward<Args>(args)...);
    case 51:
      return Func::template run<51>(std::forward<Args>(args)...);
    case 52:
      return Func::template run<52>(std::forward<Args>(args)...);
    case 53:
      return Func::template run<53>(std::forward<Args>(args)...);
    case 54:
      return Func::template run<54>(std::forward<Args>(args)...);
    case 55:
      return Func::template run<55>(std::forward<Args>(args)...);
    case 56:
      return Func::template run<56>(std::forward<Args>(args)...);
    case 57:
      return Func::template run<57>(std::forward<Args>(args)...);
    case 58:
      return Func::template run<58>(std::forward<Args>(args)...);
    case 59:
      return Func::template run<59>(std::forward<Args>(args)...);
    case 60:
      return Func::template run<60>(std::forward<Args>(args)...);
    case 61:
      return Func::template run<61>(std::forward<Args>(args)...);
    case 62:
      return Func::template run<62>(std::forward<Args>(args)...);
    case 63:
      return Func::template run<63>(std::forward<Args>(args)...);
    case 64:
      return Func::template run<64>(std::forward<Args>(args)...);
    case 65:
      return Func::template run<65>(std::forward<Args>(args)...);
    case 66:
      return Func::template run<66>(std::forward<Args>(args)...);
    case 67:
      return Func::template run<67>(std::forward<Args>(args)...);
    case 68:
      return Func::template run<68>(std::forward<Args>(args)...);
    case 69:
      return Func::template run<69>(std::forward<Args>(args)...);
    case 70:
      return Func::template run<70>(std::forward<Args>(args)...);
    case 71:
      return Func::template run<71>(std::forward<Args>(args)...);
    case 72:
      return Func::template run<72>(std::forward<Args>(args)...);
    case 73:
      return Func::template run<73>(std::forward<Args>(args)...);
    case 74:
      return Func::template run<74>(std::forward<Args>(args)...);
    case 75:
      return Func::template run<75>(std::forward<Args>(args)...);
    case 76:
      return Func::template run<76>(std::forward<Args>(args)...);
    case 77:
      return Func::template run<77>(std::forward<Args>(args)...);
    case 78:
      return Func::template run<78>(std::forward<Args>(args)...);
    case 79:
      return Func::template run<79>(std::forward<Args>(args)...);
    case 80:
      return Func::template run<80>(std::forward<Args>(args)...);
    case 81:
      return Func::template run<81>(std::forward<Args>(args)...);
    case 82:
      return Func::template run<82>(std::forward<Args>(args)...);
    case 83:
      return Func::template run<83>(std::forward<Args>(args)...);
    case 84:
      return Func::template run<84>(std::forward<Args>(args)...);
    case 85:
      return Func::template run<85>(std::forward<Args>(args)...);
    case 86:
      return Func::template run<86>(std::forward<Args>(args)...);
    case 87:
      return Func::template run<87>(std::forward<Args>(args)...);
    case 88:
      return Func::template run<88>(std::forward<Args>(args)...);
    case 89:
      return Func::template run<89>(std::forward<Args>(args)...);
    case 90:
      return Func::template run<90>(std::forward<Args>(args)...);
    case 91:
      return Func::template run<91>(std::forward<Args>(args)...);
    case 92:
      return Func::template run<92>(std::forward<Args>(args)...);
    case 93:
      return Func::template run<93>(std::forward<Args>(args)...);
    case 94:
      return Func::template run<94>(std::forward<Args>(args)...);
    case 95:
      return Func::template run<95>(std::forward<Args>(args)...);
    case 96:
      return Func::template run<96>(std::forward<Args>(args)...);
    case 97:
      return Func::template run<97>(std::forward<Args>(args)...);
    case 98:
      return Func::template run<98>(std::forward<Args>(args)...);
    case 99:
      return Func::template run<99>(std::forward<Args>(args)...);
    case 100:
      return Func::template run<100>(std::forward<Args>(args)...);
    case 101:
      return Func::template run<101>(std::forward<Args>(args)...);
    case 102:
      return Func::template run<102>(std::forward<Args>(args)...);
    case 103:
      return Func::template run<103>(std::forward<Args>(args)...);
    case 104:
      return Func::template run<104>(std::forward<Args>(args)...);
    case 105:
      return Func::template run<105>(std::forward<Args>(args)...);
    case 106:
      return Func::template run<106>(std::forward<Args>(args)...);
    case 107:
      return Func::template run<107>(std::forward<Args>(args)...);
    case 108:
      return Func::template run<108>(std::forward<Args>(args)...);
    case 109:
      return Func::template run<109>(std::forward<Args>(args)...);
    case 110:
      return Func::template run<110>(std::forward<Args>(args)...);
    case 111:
      return Func::template run<111>(std::forward<Args>(args)...);
    case 112:
      return Func::template run<112>(std::forward<Args>(args)...);
    case 113:
      return Func::template run<113>(std::forward<Args>(args)...);
    case 114:
      return Func::template run<114>(std::forward<Args>(args)...);
    case 115:
      return Func::template run<115>(std::forward<Args>(args)...);
    case 116:
      return Func::template run<116>(std::forward<Args>(args)...);
    case 117:
      return Func::template run<117>(std::forward<Args>(args)...);
    case 118:
      return Func::template run<118>(std::forward<Args>(args)...);
    case 119:
      return Func::template run<119>(std::forward<Args>(args)...);
    case 120:
      return Func::template run<120>(std::forward<Args>(args)...);
    case 121:
      return Func::template run<121>(std::forward<Args>(args)...);
    case 122:
      return Func::template run<122>(std::forward<Args>(args)...);
    case 123:
      return Func::template run<123>(std::forward<Args>(args)...);
    case 124:
      return Func::template run<124>(std::forward<Args>(args)...);
    case 125:
      return Func::template run<125>(std::forward<Args>(args)...);
    case 126:
      return Func::template run<126>(std::forward<Args>(args)...);
    case 127:
      return Func::template run<127>(std::forward<Args>(args)...);
    case 128:
      return Func::template run<128>(std::forward<Args>(args)...);
    case 129:
      return Func::template run<129>(std::forward<Args>(args)...);
    case 130:
      return Func::template run<130>(std::forward<Args>(args)...);
    case 131:
      return Func::template run<131>(std::forward<Args>(args)...);
    case 132:
      return Func::template run<132>(std::forward<Args>(args)...);
    case 133:
      return Func::template run<133>(std::forward<Args>(args)...);
    case 134:
      return Func::template run<134>(std::forward<Args>(args)...);
    case 135:
      return Func::template run<135>(std::forward<Args>(args)...);
    case 136:
      return Func::template run<136>(std::forward<Args>(args)...);
    case 137:
      return Func::template run<137>(std::forward<Args>(args)...);
    case 138:
      return Func::template run<138>(std::forward<Args>(args)...);
    case 139:
      return Func::template run<139>(std::forward<Args>(args)...);
    case 140:
      return Func::template run<140>(std::forward<Args>(args)...);
    case 141:
      return Func::template run<141>(std::forward<Args>(args)...);
    case 142:
      return Func::template run<142>(std::forward<Args>(args)...);
    case 143:
      return Func::template run<143>(std::forward<Args>(args)...);
    case 144:
      return Func::template run<144>(std::forward<Args>(args)...);
    case 145:
      return Func::template run<145>(std::forward<Args>(args)...);
    case 146:
      return Func::template run<146>(std::forward<Args>(args)...);
    case 147:
      return Func::template run<147>(std::forward<Args>(args)...);
    case 148:
      return Func::template run<148>(std::forward<Args>(args)...);
    case 149:
      return Func::template run<149>(std::forward<Args>(args)...);
    case 150:
      return Func::template run<150>(std::forward<Args>(args)...);
    case 151:
      return Func::template run<151>(std::forward<Args>(args)...);
    case 152:
      return Func::template run<152>(std::forward<Args>(args)...);
    case 153:
      return Func::template run<153>(std::forward<Args>(args)...);
    case 154:
      return Func::template run<154>(std::forward<Args>(args)...);
    case 155:
      return Func::template run<155>(std::forward<Args>(args)...);
    case 156:
      return Func::template run<156>(std::forward<Args>(args)...);
    case 157:
      return Func::template run<157>(std::forward<Args>(args)...);
    case 158:
      return Func::template run<158>(std::forward<Args>(args)...);
    case 159:
      return Func::template run<159>(std::forward<Args>(args)...);
    case 160:
      return Func::template run<160>(std::forward<Args>(args)...);
    case 161:
      return Func::template run<161>(std::forward<Args>(args)...);
    case 162:
      return Func::template run<162>(std::forward<Args>(args)...);
    case 163:
      return Func::template run<163>(std::forward<Args>(args)...);
    case 164:
      return Func::template run<164>(std::forward<Args>(args)...);
    case 165:
      return Func::template run<165>(std::forward<Args>(args)...);
    case 166:
      return Func::template run<166>(std::forward<Args>(args)...);
    case 167:
      return Func::template run<167>(std::forward<Args>(args)...);
    case 168:
      return Func::template run<168>(std::forward<Args>(args)...);
    case 169:
      return Func::template run<169>(std::forward<Args>(args)...);
    case 170:
      return Func::template run<170>(std::forward<Args>(args)...);
    case 171:
      return Func::template run<171>(std::forward<Args>(args)...);
    case 172:
      return Func::template run<172>(std::forward<Args>(args)...);
    case 173:
      return Func::template run<173>(std::forward<Args>(args)...);
    case 174:
      return Func::template run<174>(std::forward<Args>(args)...);
    case 175:
      return Func::template run<175>(std::forward<Args>(args)...);
    case 176:
      return Func::template run<176>(std::forward<Args>(args)...);
    case 177:
      return Func::template run<177>(std::forward<Args>(args)...);
    case 178:
      return Func::template run<178>(std::forward<Args>(args)...);
    case 179:
      return Func::template run<179>(std::forward<Args>(args)...);
    case 180:
      return Func::template run<180>(std::forward<Args>(args)...);
    case 181:
      return Func::template run<181>(std::forward<Args>(args)...);
    case 182:
      return Func::template run<182>(std::forward<Args>(args)...);
    case 183:
      return Func::template run<183>(std::forward<Args>(args)...);
    case 184:
      return Func::template run<184>(std::forward<Args>(args)...);
    case 185:
      return Func::template run<185>(std::forward<Args>(args)...);
    case 186:
      return Func::template run<186>(std::forward<Args>(args)...);
    case 187:
      return Func::template run<187>(std::forward<Args>(args)...);
    case 188:
      return Func::template run<188>(std::forward<Args>(args)...);
    case 189:
      return Func::template run<189>(std::forward<Args>(args)...);
    case 190:
      return Func::template run<190>(std::forward<Args>(args)...);
    case 191:
      return Func::template run<191>(std::forward<Args>(args)...);
    case 192:
      return Func::template run<192>(std::forward<Args>(args)...);
    case 193:
      return Func::template run<193>(std::forward<Args>(args)...);
    case 194:
      return Func::template run<194>(std::forward<Args>(args)...);
    case 195:
      return Func::template run<195>(std::forward<Args>(args)...);
    case 196:
      return Func::template run<196>(std::forward<Args>(args)...);
    case 197:
      return Func::template run<197>(std::forward<Args>(args)...);
    case 198:
      return Func::template run<198>(std::forward<Args>(args)...);
    case 199:
      return Func::template run<199>(std::forward<Args>(args)...);
    case 200:
      return Func::template run<200>(std::forward<Args>(args)...);
    case 201:
      return Func::template run<201>(std::forward<Args>(args)...);
    case 202:
      return Func::template run<202>(std::forward<Args>(args)...);
    case 203:
      return Func::template run<203>(std::forward<Args>(args)...);
    case 204:
      return Func::template run<204>(std::forward<Args>(args)...);
    case 205:
      return Func::template run<205>(std::forward<Args>(args)...);
    case 206:
      return Func::template run<206>(std::forward<Args>(args)...);
    case 207:
      return Func::template run<207>(std::forward<Args>(args)...);
    case 208:
      return Func::template run<208>(std::forward<Args>(args)...);
    case 209:
      return Func::template run<209>(std::forward<Args>(args)...);
    case 210:
      return Func::template run<210>(std::forward<Args>(args)...);
    case 211:
      return Func::template run<211>(std::forward<Args>(args)...);
    case 212:
      return Func::template run<212>(std::forward<Args>(args)...);
    case 213:
      return Func::template run<213>(std::forward<Args>(args)...);
    case 214:
      return Func::template run<214>(std::forward<Args>(args)...);
    case 215:
      return Func::template run<215>(std::forward<Args>(args)...);
    case 216:
      return Func::template run<216>(std::forward<Args>(args)...);
    case 217:
      return Func::template run<217>(std::forward<Args>(args)...);
    case 218:
      return Func::template run<218>(std::forward<Args>(args)...);
    case 219:
      return Func::template run<219>(std::forward<Args>(args)...);
    case 220:
      return Func::template run<220>(std::forward<Args>(args)...);
    case 221:
      return Func::template run<221>(std::forward<Args>(args)...);
    case 222:
      return Func::template run<222>(std::forward<Args>(args)...);
    case 223:
      return Func::template run<223>(std::forward<Args>(args)...);
    case 224:
      return Func::template run<224>(std::forward<Args>(args)...);
    case 225:
      return Func::template run<225>(std::forward<Args>(args)...);
    case 226:
      return Func::template run<226>(std::forward<Args>(args)...);
    case 227:
      return Func::template run<227>(std::forward<Args>(args)...);
    case 228:
      return Func::template run<228>(std::forward<Args>(args)...);
    case 229:
      return Func::template run<229>(std::forward<Args>(args)...);
    case 230:
      return Func::template run<230>(std::forward<Args>(args)...);
    case 231:
      return Func::template run<231>(std::forward<Args>(args)...);
    case 232:
      return Func::template run<232>(std::forward<Args>(args)...);
    case 233:
      return Func::template run<233>(std::forward<Args>(args)...);
    case 234:
      return Func::template run<234>(std::forward<Args>(args)...);
    case 235:
      return Func::template run<235>(std::forward<Args>(args)...);
    case 236:
      return Func::template run<236>(std::forward<Args>(args)...);
    case 237:
      return Func::template run<237>(std::forward<Args>(args)...);
    case 238:
      return Func::template run<238>(std::forward<Args>(args)...);
    case 239:
      return Func::template run<239>(std::forward<Args>(args)...);
    case 240:
      return Func::template run<240>(std::forward<Args>(args)...);
    case 241:
      return Func::template run<241>(std::forward<Args>(args)...);
    case 242:
      return Func::template run<242>(std::forward<Args>(args)...);
    case 243:
      return Func::template run<243>(std::forward<Args>(args)...);
    case 244:
      return Func::template run<244>(std::forward<Args>(args)...);
    case 245:
      return Func::template run<245>(std::forward<Args>(args)...);
    case 246:
      return Func::template run<246>(std::forward<Args>(args)...);
    case 247:
      return Func::template run<247>(std::forward<Args>(args)...);
    case 248:
      return Func::template run<248>(std::forward<Args>(args)...);
    case 249:
      return Func::template run<249>(std::forward<Args>(args)...);
    case 250:
      return Func::template run<250>(std::forward<Args>(args)...);
    case 251:
      return Func::template run<251>(std::forward<Args>(args)...);
    case 252:
      return Func::template run<252>(std::forward<Args>(args)...);
    case 253:
      return Func::template run<253>(std::forward<Args>(args)...);
    case 254:
      return Func::template run<254>(std::forward<Args>(args)...);
    case 255:
      return Func::template run<255>(std::forward<Args>(args)...);
    default:
      unreachable();
      // index shouldn't bigger than 256
  }
}
}  // namespace ylt::reflection