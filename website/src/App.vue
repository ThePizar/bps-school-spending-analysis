<template>
  <div id="app">
    <h1>{{ msg }}</h1>
    <div class="container">
      <div class="flex-item" v-for="i in [0,1]">
        <select title="School" v-model="selected[i]">
          <option v-for="school in fixedNames">{{school.school}}</option>
        </select>

        <h2>{{displayName(i)}}</h2>
        <div>
          <h3>Students: {{getSchool(i).students}}</h3>
          <h3>Spending per Student: {{formatMoney(getSchool(i).perStudent)}}</h3>
          <h3>Spending per Year: {{formatMoney(getSchool(i).perYear)}}</h3>
        </div>
        <img width="500" height="500" :src="categoryPic(displayName(i))" :title="displayName(i)">
        <img width="500" height="500" :src="historyPic(displayName(i))" :title="displayName(i)">
      </div>
    </div>
  </div>
</template>

<script>
  import avgs from './data/avgs.json'

  // register globally
  export default {
    name: 'app',
    data () {
      const baseAPI = "https://storage.googleapis.com/bps-school-info/";
      return {
        msg: 'Choose Two Schools to Compare',
        schools: avgs,
        selected: [avgs[2].school, avgs[1].school],
        categoryAPI: baseAPI + "pics/category/v2/",
        historyAPI: baseAPI + "pics/history/v2/"
      }
    },
    mounted () {
      //this.loadSpreadsheet()
    },
    methods: {
      getSchool (i) {
        return this.fixedNames.find(school => {
          return school.school === this.displayName(i);
        })
      },
      categoryPic (name) {
        return this.categoryAPI + encodeURIComponent(name) + '.png'
      },
      historyPic (name) {
        return this.historyAPI + encodeURIComponent(name) + '.png'
      },
      displayName (i) {
        return this.selected[i]
      },
      formatMoney(money) {
        let strMoney = money + "";
        for (let i = strMoney.length - 3; i > 0; i -=3) {
          strMoney = strMoney.slice(0, i) + "," + strMoney.slice(i)
        }
        return "$" + strMoney
      }
    },
    computed: {
      fixedNames () {
        return this.schools.map(school => {
          school.school = school.school.replace("/", " ");
          return school
        }).sort((a, b) => {
          let nameA = a.school.toUpperCase(); // ignore upper and lowercase
          let nameB = b.school.toUpperCase(); // ignore upper and lowercase
          if (nameA < nameB) {
            return -1;
          }
          if (nameA > nameB) {
            return 1;
          }

          // names must be equal
          return 0;
        })
      }
    }
  }
</script>

<style>
#app {
  font-family: 'Avenir', Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
  margin-top: 60px;
}

h1, h2 {
  font-weight: normal;
}

h1 {
  font-size: 60px;
}

ul {
  list-style-type: none;
  padding: 0;
}

li {
  display: inline-block;
  margin: 0 10px;
}

.container{
  display: flex;
}

.flex-item{
  flex-grow: 1;
}

a {
  color: #42b983;
}
</style>
